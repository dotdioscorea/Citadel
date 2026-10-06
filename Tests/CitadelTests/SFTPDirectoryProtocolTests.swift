@testable import Citadel
import Logging
import NIO
import NIOConcurrencyHelpers
import XCTest

final class SFTPDirectoryProtocolTests: XCTestCase {
    func testReadFailureClosesHandleAndPreservesStatus() async throws {
        try await withPeer(.readFailure) { client, state, _ in
            do {
                _ = try await client.listDirectory(atPath: "/fixture")
                XCTFail("A failed READDIR must fail the listing.")
            } catch let status as SFTPMessage.Status {
                XCTAssertEqual(status.errorCode, .permissionDenied)
            }
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testUnexpectedReadSuccessStatusFailsAndClosesHandle() async throws {
        try await withPeer(.unexpectedReadResponse) { client, state, _ in
            do {
                _ = try await client.listDirectory(atPath: "/fixture")
                XCTFail("OK is not an end-of-directory response.")
            } catch SFTPError.invalidResponse {}
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testCloseFailureIsSurfacedAfterCompleteListing() async throws {
        try await withPeer(.closeFailure) { client, state, _ in
            do {
                _ = try await client.listDirectory(atPath: "/fixture")
                XCTFail("A rejected CLOSE must remain visible to the caller.")
            } catch let status as SFTPMessage.Status {
                XCTAssertEqual(status.errorCode, .failure)
            }
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testReadAndCloseFailuresRetainBothErrors() async throws {
        try await withPeer(.readAndCloseFailure) { client, state, _ in
            do {
                _ = try await client.listDirectory(atPath: "/fixture")
                XCTFail("The failed operation and failed cleanup must not become success.")
            } catch let error as SFTPDirectoryCleanupError {
                XCTAssertEqual((error.operationError as? SFTPMessage.Status)?.errorCode, .permissionDenied)
                XCTAssertEqual((error.cleanupError as? SFTPMessage.Status)?.errorCode, .failure)
            }
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testCancellationDuringReadClosesHandleBeforeThrowing() async throws {
        try await withPeer(.blockedRead) { client, state, gate in
            let listing = Task { try await client.listDirectory(atPath: "/fixture") }
            await self.fulfillment(of: [gate.arrived], timeout: 3)
            listing.cancel()
            gate.release.succeed(())
            do {
                _ = try await listing.value
                XCTFail("A cancelled listing must not return partial success.")
            } catch is CancellationError {}
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testEmptyDirectoryClosesItsHandle() async throws {
        try await withPeer(.empty) { client, state, _ in
            let listing = try await client.listDirectory(atPath: "/fixture")
            XCTAssertTrue(listing.isEmpty)
            XCTAssertEqual(state.withLockedValue { $0.readRequests }, 1)
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testClosedChannelDuringReadPreservesConnectionError() async throws {
        try await withPeer(.closeDuringRead) { client, state, _ in
            do {
                _ = try await client.listDirectory(atPath: "/fixture")
                XCTFail("A closed channel must fail the pending listing.")
            } catch SFTPError.connectionClosed {}
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 0)
        }
    }

    func testNonStatusCloseReplyRemainsAProtocolFailure() async throws {
        try await withPeer(.closeNonStatus) { client, state, _ in
            do {
                _ = try await client.listDirectory(atPath: "/fixture")
                XCTFail("CLOSE must receive a status response.")
            } catch SFTPError.invalidResponse {}
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    func testAllReadPagesAreRetainedAndHandleClosesOnce() async throws {
        try await withPeer(.success) { client, state, _ in
            let listing = try await client.listDirectory(atPath: "/fixture")
            XCTAssertEqual(listing.flatMap(\.components).map(\.filename), ["report.md", "agent.json"])
            XCTAssertEqual(state.withLockedValue { $0.readRequests }, 3)
            XCTAssertEqual(state.withLockedValue { $0.closeRequests }, 1)
        }
    }

    private func withPeer(
        _ mode: DirectoryPeerMode,
        body: (SFTPClient, NIOLockedValueBox<DirectoryPeerState>, DirectoryReadGate) async throws -> Void
    ) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let state = NIOLockedValueBox(DirectoryPeerState(mode: mode))
        let gate = DirectoryReadGate(
            arrived: XCTestExpectation(description: "READDIR received"),
            release: group.next().makePromise()
        )
        if mode != .blockedRead {
            gate.release.succeed(())
        }
        var server: Channel?
        var connection: Channel?
        var operationError: Error?
        do {
            let listener = try await ServerBootstrap(group: group).childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers(
                        ByteToMessageHandler(SFTPMessageParser()),
                        MessageToByteHandler(SFTPMessageSerializer()),
                        DirectoryPeerHandler(state: state, gate: gate)
                    )
                }
            }.bind(host: "127.0.0.1", port: 0).get()
            server = listener
            let port = try XCTUnwrap(listener.localAddress?.port)
            let responses = SFTPResponses(sftpVersion: group.next().makePromise())
            let logger = Logger(label: "citadel.directory-lifecycle-test")
            let channel = try await ClientBootstrap(group: group).channelInitializer { channel in
                responses.sftpVersion.succeed(.init(version: .v3, extensionData: []))
                return channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandlers(
                        ByteToMessageHandler(SFTPMessageParser()),
                        MessageToByteHandler(SFTPMessageSerializer()),
                        SFTPClientInboundHandler(responses: responses, logger: logger)
                    )
                }
            }.connect(host: "127.0.0.1", port: port).get()
            connection = channel
            channel.closeFuture.whenComplete { _ in responses.close() }
            let client = SFTPClient(channel: channel, responses: responses, logger: logger)
            try await body(client, state, gate)
        } catch {
            operationError = error
        }
        // Also release an unused gate if setup failed before the test body could do so.
        gate.release.succeed(())
        for channel in [connection, server].compactMap(\.self) where channel.isActive {
            do { try await channel.close() }
            catch { XCTFail("Test channel cleanup failed: \(error)") }
        }
        do { try await group.shutdownGracefully() }
        catch { XCTFail("Test event-loop cleanup failed: \(error)") }
        if let operationError {
            throw operationError
        }
    }
}

private enum DirectoryPeerMode {
    case success, empty, readFailure, unexpectedReadResponse, closeFailure, readAndCloseFailure, blockedRead
    case closeDuringRead, closeNonStatus
}

private struct DirectoryPeerState {
    let mode: DirectoryPeerMode
    var readRequests = 0
    var closeRequests = 0
}

private struct DirectoryReadGate {
    let arrived: XCTestExpectation
    let release: EventLoopPromise<Void>
}

private final class DirectoryPeerHandler: ChannelInboundHandler {
    typealias InboundIn = SFTPMessage
    let state: NIOLockedValueBox<DirectoryPeerState>
    let gate: DirectoryReadGate

    init(state: NIOLockedValueBox<DirectoryPeerState>, gate: DirectoryReadGate) {
        self.state = state
        self.gate = gate
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let message = unwrapInboundIn(data)
        switch message {
        case let .realpath(request):
            context.writeAndFlush(NIOAny(SFTPMessage.name(.init(requestId: request.requestId, components: [
                .init(filename: request.path, longname: request.path, attributes: .init())
            ]))), promise: nil)
        case let .opendir(request):
            context.writeAndFlush(NIOAny(SFTPMessage.handle(.init(
                requestId: request.requestId, handle: ByteBuffer(string: "directory-fixture")
            ))), promise: nil)
        case let .readdir(request):
            let (mode, page) = state.withLockedValue { state in
                state.readRequests += 1
                return (state.mode, state.readRequests)
            }
            switch mode {
            case .readFailure, .readAndCloseFailure:
                reply(.permissionDenied, id: request.requestId, context: context)
            case .unexpectedReadResponse:
                reply(.ok, id: request.requestId, context: context)
            case .empty:
                reply(.eof, id: request.requestId, context: context)
            case .blockedRead:
                gate.arrived.fulfill()
                let boundContext = NIOLoopBound(context, eventLoop: context.eventLoop)
                gate.release.futureResult.whenSuccess {
                    let context = boundContext.value
                    context.writeAndFlush(NIOAny(SFTPMessage.name(.init(requestId: request.requestId, components: [
                        .init(filename: "partial.md", longname: "partial.md", attributes: .init())
                    ]))), promise: nil)
                }
            case .closeDuringRead:
                context.close(promise: nil)
            case .success, .closeFailure, .closeNonStatus:
                if page <= 2 {
                    let name = page == 1 ? "report.md" : "agent.json"
                    context.writeAndFlush(NIOAny(SFTPMessage.name(.init(requestId: request.requestId, components: [
                        .init(filename: name, longname: name, attributes: .init())
                    ]))), promise: nil)
                } else {
                    reply(.eof, id: request.requestId, context: context)
                }
            }
        case let .closeFile(request):
            let mode = state.withLockedValue { state in
                state.closeRequests += 1
                return state.mode
            }
            if mode == .closeNonStatus {
                context.writeAndFlush(NIOAny(SFTPMessage.handle(.init(
                    requestId: request.requestId, handle: ByteBuffer(string: "invalid-close-reply")
                ))), promise: nil)
            } else {
                reply(
                    mode == .closeFailure || mode == .readAndCloseFailure ? .failure : .ok,
                    id: request.requestId,
                    context: context
                )
            }
        default:
            XCTFail("Unexpected directory request: \(message)")
            context.close(promise: nil)
        }
    }

    private func reply(_ code: SFTPStatusCode, id: UInt32, context: ChannelHandlerContext) {
        context.writeAndFlush(NIOAny(SFTPMessage.status(.init(
            requestId: id, errorCode: code, message: "Synthetic directory peer", languageTag: "EN"
        ))), promise: nil)
    }
}
