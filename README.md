See leader and follower state machine diagrams [here](https://www.figma.com/board/UXkhn2g3OArWUgZJCK5Ilv/PerCorr-Project-2---File-Transmission?node-id=0-1&t=E5eHPbcr2bWSYLFn-1)

## Sending a File Over the Network

The process of sending a file over the network is modeled with a Leader and a Follower, each with its own state machine. The Leader initiates the transfer by sending a `Restarted?()` message, and the Follower responds with a `RestartedReply(restarted)` message. If the Follower indicates that it has restarted, the Leader proceeds to send the file in a single `Blob(payload)` message. The Follower acknowledges receipt with a `BlobReply(result)` message. The state machines handle retries, link errors, and timeouts to ensure reliable file transmission despite failures. The 3 failure modes considered are:
- **Message loss and process crashes.** A message can be dropped by the network, or either process can crash and restart at any point. 
- **Link-level failures.** These are reported as error codes from the `send(2)` and `recv(2)` system calls, for example a refused or reset connection. An error does not prove the message was not delivered, so it is never treated as a conclusion. Both processes go back to the sending state and retry, reconnecting first if needed. 
- **Message duplication.** Because messages are retransmitted, the same message can arrive more than once. The Leader ignores any message that is not the reply it is waiting for.

Note that strategies like chunking are not considered in this initial model, but will be included in later iterations.

For running, see `Makefile`. Run `make help` to get started.