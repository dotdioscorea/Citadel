@testable import Citadel
import NIOConcurrencyHelpers
import XCTest

extension EndToEndTests {
    func testDirectoryListingClosesEveryServerHandle() async throws {
        let handles = NIOLockedValueBox(0)
        try await runTest(
            perform: { server, client in
                server.enableSFTP(withDelegate: DirectoryLifecycleDelegate(handles: handles))
                let sftp = try await client.openSFTP()
                for _ in 0 ..< 4 {
                    let listing = try await sftp.listDirectory(atPath: "/fixture")
                    XCTAssertEqual(listing.flatMap(\.components).map(\.filename), ["report.md"])
                    XCTAssertEqual(
                        handles.withLockedValue { $0 },
                        0,
                        "A completed listing must release its remote directory handle."
                    )
                }
                try await sftp.close()
            },
            matchingError: { _ in false },
            expectsFailure: false
        )
    }
}

private struct DirectoryLifecycleDelegate: SFTPDelegate {
    let handles: NIOLockedValueBox<Int>

    func realPath(for path: String, context: SSHContext) async throws -> [SFTPPathComponent] {
        [SFTPPathComponent(filename: path, longname: path, attributes: .init())]
    }

    func openDirectory(atPath path: String, context: SSHContext) async throws -> SFTPDirectoryHandle {
        DirectoryLifecycleHandle(handles: handles)
    }
}

private final class DirectoryLifecycleHandle: SFTPDirectoryHandle {
    let handles: NIOLockedValueBox<Int>

    init(handles: NIOLockedValueBox<Int>) {
        self.handles = handles
        handles.withLockedValue { $0 += 1 }
    }

    deinit {
        handles.withLockedValue { $0 -= 1 }
    }

    func listFiles(context: SSHContext) async throws -> [SFTPFileListing] {
        [.init(path: [.init(filename: "report.md", longname: "report.md", attributes: .init())])]
    }
}
