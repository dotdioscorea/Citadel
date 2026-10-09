@testable import Citadel
import Foundation
import NIO
@preconcurrency import NIOSSH
import XCTest

@available(macOS 15.0, *)
final class PTYCommandTests: XCTestCase {
    func testCommandUsesExecAfterPTYWithoutWritingCommandToInput() async throws {
        try await runPTY(command: "printf remote-command") { state in
            XCTAssertEqual(state.events, ["pty", "exec"])
            XCTAssertEqual(state.command, "printf remote-command")
            XCTAssertEqual(state.input, [])
        }
    }

    func testDefaultPTYStillRequestsInteractiveShellWithoutInput() async throws {
        try await runPTY(command: nil) { state in
            XCTAssertEqual(state.events, ["pty", "shell"])
            XCTAssertNil(state.command)
            XCTAssertEqual(state.input, [])
        }
    }

    func testRejectedExecPreservesChannelFailure() async throws {
        do {
            try await runPTY(command: "rejected", rejectsExec: true) { _ in
                XCTFail("Rejected exec must not supply terminal output.")
            }
            XCTFail("Rejected exec must fail visibly.")
        } catch CitadelError.channelFailure {
            // The primary request failure must survive channel cleanup.
        }
    }

    func testRemoteSuccessfulExitCompletesNormally() async throws {
        try await runPTY(command: "completed", exitCode: 0) { state in
            XCTAssertEqual(state.events, ["pty", "exec"])
            XCTAssertEqual(state.input, [])
        }
    }

    func testDefaultShellRemoteExitCompletesNormally() async throws {
        try await runPTY(command: nil, exitCode: 0) { state in
            XCTAssertEqual(state.events, ["pty", "shell"])
        }
    }

    func testRejectedPTYFailsVisibly() async throws {
        do {
            try await runPTY(command: "rejected-pty", rejectsPTY: true) { _ in
                XCTFail("A refused PTY must fail visibly.")
            }
            XCTFail("A refused PTY must not succeed.")
        } catch CitadelError.channelFailure {
            // PTY refusal is preserved even when the remote closes the channel.
        }
    }

    func testRemoteFailedExitPreservesStatus() async throws {
        do {
            try await runPTY(command: "failed", exitCode: 42) { state in
                XCTAssertEqual(state.input, [])
            }
            XCTFail("A nonzero remote exit must fail visibly.")
        } catch let error as SSHClient.CommandFailed {
            XCTAssertEqual(error.exitCode, 42)
        }
    }

    private func runPTY(
        command: String?, rejectsExec: Bool = false, rejectsPTY: Bool = false, exitCode: Int? = nil,
        assertBeforeInput: @escaping @Sendable (PTYRequestRecord.State) -> Void
    ) async throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let record = PTYRequestRecord()
        let auth = AuthDelegate(supportedAuthenticationMethods: .password) { request, promise in
            if case .password(.init(password: "test")) = request.request, request.username == "citadel" {
                promise.succeed(.success)
            } else {
                promise.succeed(.failure)
            }
        }
        let server = try await ServerBootstrap(group: group)
            .childChannelInitializer { channel in
                channel.pipeline.addHandler(NIOSSHHandler(
                    role: .server(.init(
                        hostKeys: [NIOSSHPrivateKey(p521Key: .init())], userAuthDelegate: auth
                    )),
                    allocator: channel.allocator,
                    inboundChildChannelInitializer: { child, type in
                        guard case .session = type else { return child.eventLoop.makeFailedFuture(CitadelError.unsupported) }
                        return child.pipeline.addHandler(PTYRequestHandler(
                            record: record, rejectsExec: rejectsExec, rejectsPTY: rejectsPTY, exitCode: exitCode
                        ))
                    }
                ))
            }
            .bind(host: "127.0.0.1", port: 0).get()
        let port = try XCTUnwrap(server.localAddress?.port)
        let client = try await SSHClient.connect(
            host: "127.0.0.1", port: port,
            authenticationMethod: .passwordBased(username: "citadel", password: "test"),
            hostKeyValidator: .acceptAnything(), reconnect: .never
        )
        do {
            try await withThrowingTaskGroup(of: Void.self) { tasks in
                tasks.addTask {
                    try await withTaskCancellationHandler {
                        try await client.withPTY(.init(
                            wantReply: true, term: "xterm-256color",
                            terminalCharacterWidth: 80, terminalRowHeight: 24,
                            terminalPixelWidth: 0, terminalPixelHeight: 0, terminalModes: .init([:])
                        ), command: command) { inbound, outbound in
                            var iterator = inbound.makeAsyncIterator()
                            let ready = try await iterator.next()
                            guard case let .stdout(buffer)? = ready else {
                                return XCTFail("The requested terminal must produce stdout.")
                            }
                            XCTAssertEqual(String(buffer: buffer), "remote-ready")
                            assertBeforeInput(record.snapshot)
                            if exitCode != nil {
                                while try await iterator.next() != nil {}
                                return
                            }
                            try await outbound.write(ByteBuffer(string: "user-input"))
                            let reply = try await iterator.next()
                            guard case let .stdout(buffer)? = reply else {
                                return XCTFail("Intentional terminal input must reach the remote endpoint.")
                            }
                            XCTAssertEqual(String(buffer: buffer), "input-received")
                            XCTAssertEqual(record.snapshot.input, ["user-input"])
                        }
                    } onCancel: {
                        Task { try? await client.close() }
                    }
                }
                tasks.addTask {
                    try await Task.sleep(for: .seconds(8))
                    throw PTYTestTimeout()
                }
                defer { tasks.cancelAll() }
                try await tasks.next()
            }
        } catch {
            try? await client.close()
            try? await server.close().get()
            try? await group.shutdownGracefully()
            throw error
        }
        try await client.close()
        try await server.close().get()
        try await group.shutdownGracefully()
    }
}

private struct PTYTestTimeout: Error {}

private final class PTYRequestRecord: @unchecked Sendable {
    struct State: Sendable {
        var events: [String] = []
        var command: String?
        var input: [String] = []
    }
    private let lock = NSLock()
    private var state = State()

    var snapshot: State { lock.withLock { state } }

    func update(_ change: (inout State) -> Void) {
        lock.withLock { change(&state) }
    }
}

private final class PTYRequestHandler: ChannelInboundHandler {
    typealias InboundIn = SSHChannelData
    private let record: PTYRequestRecord
    private let rejectsExec: Bool
    private let rejectsPTY: Bool
    private let exitCode: Int?

    init(record: PTYRequestRecord, rejectsExec: Bool, rejectsPTY: Bool, exitCode: Int?) {
        self.record = record
        self.rejectsExec = rejectsExec
        self.rejectsPTY = rejectsPTY
        self.exitCode = exitCode
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case is SSHChannelRequestEvent.PseudoTerminalRequest:
            record.update { $0.events.append("pty") }
            if rejectsPTY {
                refuse(context: context)
            } else {
                context.triggerUserOutboundEvent(ChannelSuccessEvent(), promise: nil)
            }
        case let request as SSHChannelRequestEvent.ExecRequest:
            record.update { $0.events.append("exec"); $0.command = request.command }
            if rejectsExec || rejectsPTY {
                refuse(context: context)
            } else {
                context.triggerUserOutboundEvent(ChannelSuccessEvent(), promise: nil)
                write("remote-ready", context: context)
                finishRemoteCommand(context: context)
            }
        case is SSHChannelRequestEvent.ShellRequest:
            record.update { $0.events.append("shell") }
            context.triggerUserOutboundEvent(ChannelSuccessEvent(), promise: nil)
            write("remote-ready", context: context)
            finishRemoteCommand(context: context)
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if case let .byteBuffer(buffer) = unwrapInboundIn(data).data {
            record.update { $0.input.append(String(buffer: buffer)) }
            write("input-received", context: context)
        }
    }

    private func write(_ text: String, context: ChannelHandlerContext) {
        context.writeAndFlush(NIOAny(SSHChannelData(type: .channel, data: .byteBuffer(ByteBuffer(string: text)))), promise: nil)
    }

    private func finishRemoteCommand(context: ChannelHandlerContext) {
        guard let exitCode else { return }
        let channel = context.channel
        channel.triggerUserOutboundEvent(SSHChannelRequestEvent.ExitStatus(exitStatus: exitCode))
            .whenComplete { _ in channel.close(promise: nil) }
    }

    private func refuse(context: ChannelHandlerContext) {
        let channel = context.channel
        channel.triggerUserOutboundEvent(ChannelFailureEvent()).whenComplete { _ in channel.close(promise: nil) }
    }
}
