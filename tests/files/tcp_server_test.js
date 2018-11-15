function test_request_handler(command, connection) {
    print("DEBUG: received request", command);
    if (command.startsWith("wait")) {
        let timer = Components.Constructor("@mozilla.org/timer;1", "nsITimer", "initWithCallback");
        let reply_event = {
            connection: connection,
            notify: function() {
                this.connection.reply(command);
            }
        };
        timer(reply_event, 2000, Components.interfaces.nsITimer.TYPE_ONE_SHOT);
    } else if (command.startsWith("quit")) {
        connection.reply(command);
        s.stop();
    } else {
        connection.reply(command);
    }
}


const s = new TCPServer(test_request_handler, 27357);
s.start();
print("port", s.port);
s.loop();
s.wait();
