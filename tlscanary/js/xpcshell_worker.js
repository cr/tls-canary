/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

"use strict";

const {classes: Cc, interfaces: Ci, utils: Cu, results: Cr, Constructor: CC} = Components;

const DEFAULT_TIMEOUT = 10000;
const thread_manager = Cc["@mozilla.org/thread-manager;1"].getService(Ci.nsIThreadManager);
const main_thread = thread_manager.mainThread;

Cu.import("resource://gre/modules/Services.jsm");
Cu.import("resource://gre/modules/XPCOMUtils.jsm");
Cu.import("resource://gre/modules/NetUtil.jsm");
Cu.import("resource://gre/modules/AppConstants.jsm");

const nsINSSErrorsService = Ci.nsINSSErrorsService;
let nssErrorsService = Cc['@mozilla.org/nss_errors_service;1'].getService(nsINSSErrorsService);


function uuid4() {
  function rnd(bits) {
    return Math.floor((1 + Math.random()) * 2**bits).toString(16).substring(1);
  }
  return `${rnd(32)}-${rnd(16)}-${rnd(16)}-${rnd(16)}-${rnd(48)}`;
}


// This is a global worker ID that is sent with every message to the Python world
// It can be overridden by the `setworkerid` command. The Python world will usually
// set this to a UUID1 string to synchronize the two worlds.
let worker_id = uuid4();


function set_worker_id(id) {
    worker_id = id;
}


function get_runtime_info() {
    return {
        nssInfo: Cc["@mozilla.org/security/nssversion;1"].getService(Ci.nsINSSVersion),
        appConstants: AppConstants
    };
}


function set_prefs(prefs) {
    for (let key in prefs) {
        let prop = prefs[key].split(";")[0];
        let value = prefs[key].split(";")[1];

        // Pref values are passed in as strings and must be examined
        // to determine the intended types and values.
        let type = "string"; // default
        if (value === "true" || value === "false") type = "boolean";
        if (!isNaN(value)) type = "number";
        if (value === undefined) type = "undefined";

        switch (type) {
            case "boolean":
                Services.prefs.setBoolPref(prop, value === "true" ? 1 : 0);
                break;
            case "number":
                Services.prefs.setIntPref(prop, value);
                break;
            case "string":
                Services.prefs.setPref(prop, value);
                break;
            default:
                throw "Unsupported pref type " + type;
        }
    }
}


function set_profile(profile_path) {
    let file = Cc["@mozilla.org/file/local;1"]
        .createInstance(Ci.nsILocalFile);
    file.initWithPath(profile_path);
    let dir_service = Cc["@mozilla.org/file/directory_service;1"]
        .getService(Ci.nsIProperties);
    let provider = {
        getFile: function(prop, persistent) {
            persistent.value = true;
            if (prop === "ProfD" || prop === "ProfLD" || prop === "ProfDS" ||
                prop === "ProfLDS" || prop === "PrefD" || prop === "TmpD") {
                return file.clone();
            }
            return null;
        },
        QueryInterface: function(iid) {
            if (iid.equals(Ci.nsIDirectoryServiceProvider) ||
                iid.equals(Ci.nsISupports)) {
                return this;
            }
            throw Cr.NS_ERROR_NO_INTERFACE;
        }
    };
    dir_service.QueryInterface(Ci.nsIDirectoryService)
        .registerProvider(provider);

    // The methods of 'provider' will retain this scope so null out
    // everything to avoid spurious leak reports.
    profile_path = null;
    dir_service = null;
    provider = null;

    return file.clone();
}


function collect_request_info(xhr, report_certs) {
    // Much of this is documented in https://developer.mozilla.org/en-US/docs/Web/API/
    // XMLHttpRequest/How_to_check_the_secruity_state_of_an_XMLHTTPRequest_over_SSL

    let info = {};
    info.status = xhr.channel.QueryInterface(Ci.nsIRequest).status;
    info.original_uri = xhr.channel.originalURI.asciiSpec;
    info.uri = xhr.channel.URI.asciiSpec;

    try {
        info.error_class = nssErrorsService.getErrorClass(info.status);
    } catch (e) {
        info.error_class = null;
    }

    info.security_info_status = false;
    info.transport_security_info_status = false;
    info.ssl_status_status = false;

    // Try to query security info
    let sec_info = xhr.channel.securityInfo;
    if (sec_info === null) return info;
    info.security_info_status = true;

    if (sec_info instanceof Ci.nsITransportSecurityInfo) {
        sec_info.QueryInterface(Ci.nsITransportSecurityInfo);
        info.transport_security_info_status = true;
        info.security_state = sec_info.securityState;
        info.security_description = sec_info.shortSecurityDescription;
        info.raw_error = sec_info.errorMessage;
    }

    if (sec_info instanceof Ci.nsISSLStatusProvider) {
        info.ssl_status_status = false;
        let ssl_status = sec_info.QueryInterface(Ci.nsISSLStatusProvider).SSLStatus;
        if (ssl_status != null) {
            info.ssl_status_status = true;
            info.ssl_status = ssl_status.QueryInterface(Ci.nsISSLStatus);
            // TODO: Find way to extract this py-side.
            try {
                let usages = {};
                let usages_string = {};
                info.ssl_status.server_cert.getUsagesString(true, usages, usages_string);
                info.certified_usages = usages_string.value;
            } catch (e) {
                info.certified_usages = null;
            }
        }
    }

    if (info.ssl_status_status && report_certs) {
        let server_cert = info.ssl_status.serverCert;
        let cert_chain = [];
        if (server_cert.sha1Fingerprint) {
            cert_chain.push(server_cert.getRawDER({}));
            let chain = server_cert.getChain().enumerate();
            while (chain.hasMoreElements()) {
                let child_cert = chain.getNext().QueryInterface(Ci.nsISupports)
                    .QueryInterface(Ci.nsIX509Cert);
                cert_chain.push(child_cert.getRawDER({}));
            }
        }
        info.certificate_chain_length = cert_chain.length;
        info.certificate_chain = cert_chain;
    } else {
        info.certificate_chain_length = 0;
        info.certificate_chain = null;
    }

    if (info.ssl_status_status) {
        // Some values might be missing from the connection state, for example due
        // to a broken SSL handshake. Try to catch exceptions before report_result's
        // JSON serializing does.
        let sane_ssl_status = {};
        info.ssl_status_errors = [];
        for (let key in info.ssl_status) {
            if (!info.ssl_status.hasOwnProperty(key)) continue;
            try {
                sane_ssl_status[key] = JSON.parse(JSON.stringify(info.ssl_status[key]));
            } catch (e) {
                sane_ssl_status[key] = null;
                info.ssl_status_errors.push({key: e.toString()});
            }
        }
        info.ssl_status = sane_ssl_status;
    }

    return info;
}


function scan_host(args, response_cb) {

    let host = args.host;

    // Prepend https:// scheme if there is no scheme defined in the host name
    const scheme_sep = host.indexOf("://");
    if (scheme_sep < 0 || scheme_sep > 10) host = "https://" + host;

    let report_certs = args.include_certificates === true;

    function load_handler(msg) {
        if (msg.target.readyState === 4) {
            response_cb(true, {origin: "load_handler", info: collect_request_info(msg.target, report_certs)});
        } else {
            response_cb(false, {origin: "load_handler", info: collect_request_info(msg.target, report_certs)});
        }
    }

    function error_handler(msg) {
        response_cb(false, {origin: "error_handler", info: collect_request_info(msg.target, report_certs)});
    }

    function abort_handler(msg) {
        response_cb(false, {origin: "abort_handler", info: collect_request_info(msg.target, report_certs)});
    }

    function timeout_handler(msg) {
        response_cb(false, {origin: "timeout_handler", info: collect_request_info(msg.target, report_certs)});
    }

    // This gets called when a redirect happens.
    function RedirectStopper() {}
    RedirectStopper.prototype = {
        asyncOnChannelRedirect: function (oldChannel, newChannel, flags, callback) {
            // This callback prevents redirects, and the request's error handler will be called.
            callback.onRedirectVerifyCallback(Cr.NS_ERROR_ABORT);
        },
        getInterface: function (iid) {
            return this.QueryInterface(iid);
        },
        QueryInterface: XPCOMUtils.generateQI([Ci.nsIChannelEventSink])
    };

    let request = Cc["@mozilla.org/xmlextras/xmlhttprequest;1"].createInstance(Ci.nsIXMLHttpRequest);
    try {
        request.mozBackgroundRequest = true;
        request.open("HEAD", host, true);
        request.timeout = args.timeout ? args.timeout * 1000 : DEFAULT_TIMEOUT;
        request.channel.loadFlags |= Ci.nsIRequest.LOAD_ANONYMOUS
            | Ci.nsIRequest.LOAD_BYPASS_CACHE
            | Ci.nsIRequest.INHIBIT_PERSISTENT_CACHING
            | Ci.nsIRequest.VALIDATE_NEVER;
        request.channel.notificationCallbacks = new RedirectStopper();
        request.addEventListener("load", load_handler, false);
        request.addEventListener("error", error_handler, false);
        request.addEventListener("abort", abort_handler, false);
        request.addEventListener("timeout", timeout_handler, false);
        request.send(null);
    } catch (error) {
        // This is supposed to catch malformed host names, but could
        // potentially mask other errors.
        response_cb(false, {origin: "request_error", error: error.message,
            info: collect_request_info(request, false)});
    }
}

function do_test(args, response_cb) {
    if (args.count_to) {
        print("DEBUG: Counting to", args.count_to);
        for (let x = 1; x <= args.count_to; x++) {
            for (let y = 0; y < 100000; y += 2) { y--; }  // Just waste some time
            if (args.count_debug) {
                print("DEBUG: Counting", x);
            }
        }
        response_cb(true, "ACK");
    } else if (args.sleep) {
        print("DEBUG: sleeping for " + args.sleep + " seconds");
        let timer = CC("@mozilla.org/timer;1", "nsITimer", "initWithCallback");
        let event = {
            notify: function() {
                response_cb(true, "ACK");
            }
        };
        timer(event, 1000 * args.sleep, Ci.nsITimer.TYPE_ONE_SHOT);
    } else {
        response_cb(false, {origin: "do_test", error: null, info: "improper test command"});
    }

}



// Command object definition. Must be in-sync with Python world.
// This is used for keeping state throughout async command handling.
function Command(json_string, connection) {
    this.connection = connection;
    let parsed_command = {};
    this.id = parsed_command.id ? parsed_command.id : uuid4();
    this.start_time = new Date();
    try {
        parsed_command = JSON.parse(json_string);
    } catch (error) {
        this.mode = null;
        this.args = null;
        this.original_cmd = null;
        this.reply(false, {origin: "Command", error: error.message, info: "invalid JSON"});
        throw error;
    }
    this.mode = parsed_command.mode;
    this.args = parsed_command.args;
    this.original_cmd = parsed_command;
}

// Even though it's a prototype method it will require bind when passed as callback.
Command.prototype.reply = function _report_result(success, result) {
    // Send a response back to the python world
    const reply = JSON.stringify({
        "id": this.id,
        "worker_id": worker_id,
        "original_cmd": this.original_cmd,
        "success": success,
        "result": result,
        "command_time": this.start_time.getTime(),
        "response_time": new Date().getTime(),
    });
    send_reply(reply, this.connection);
// TODO:    while (main_thread.hasPendingEvents()) main_thread.processNextEvent(true);
};

Command.prototype.handle = function _handle() {
    try {
        // Every command must be acknowledged with result "ACK,n"
        // where n is the number of pending command responses.
        switch (this.mode) {
            case "setid":
                set_worker_id(this.args.id);
                this.reply(true, "ACK");
                break;
            case "info":
                this.reply(true, get_runtime_info());
                break;
            case "useprofile":
                set_profile(this.args.path);
                this.reply(true, "ACK");
                break;
            case "setprefs":
                set_prefs(this.args.prefs);
                this.reply(true, "ACK");
                break;
            case "scan":
                // .bind is required for callback to avoid
                // 'this is undefined' when called from request handlers.
                scan_host(this.args, this.reply.bind(this));
                break;
            case "test":
                // Command mode used for unit testing
                do_test(this.args, this.reply.bind(this));
                break;
            case "quit":
                script_running = false;
                this.reply(true, "ACK");
                break;
            case "wakeup":
                wakeup_pings = this.args.pings;
                this.reply(true, "ACK");
                break;
            default:
                this.reply(false, {origin: "Command.handle", error: null, info: "unknown mode"});
        }
    } catch (error) {
        this.reply(false, {origin: "Command.handle", error: error.message, info: "internal error"});
        throw error;
    }
};


/**
 * Glue code between socket server requests and TLS Canary functions.
 */

function handle_request(request, connection) {
    print("DEBUG: Handling request:", request);
    try {
        let cmd = new Command(request, connection);
        cmd.handle();
    } catch (error) {
        print("ERROR: Unable to handle command:", error);
        connection.close()
    }
}

function send_reply(reply_string, connection) {
    connection.reply(reply_string);
}

/**
 * Implementation of a TCP socket-based command server. It is heavily inspired by
 * https://dxr.mozilla.org/mozilla-central/source/netwerk/test/httpserver/httpd.js
 * where you'll likely find the solution of all current issues with this code.
 */

const ServerSocket = CC("@mozilla.org/network/server-socket;1",
                        "nsIServerSocket",
                        "init");
const ConverterInputStream = CC("@mozilla.org/intl/converter-input-stream;1",
                                "nsIConverterInputStream",
                                "init");
const ConverterOutputStream = CC("@mozilla.org/intl/converter-output-stream;1",
                                 "nsIConverterOutputStream",
                                 "init");


let SocketListener = {
    onSocketAccepted(socket, transport) {
        print("DEBUG: Connection from port", transport.host, transport.port);
        try {
            transport.setTimeout(Cr.TIMEOUT_CONNECT, 60);
            transport.setTimeout(Cr.TIMEOUT_READ_WRITE, 60);
            // let input_stream = transport.openInputStream(Cr.OPEN_UNBUFFERED | Cr.OPEN_BLOCKING, 0, 0)
            let input_stream = transport.openInputStream(0, 0, 0)
                .QueryInterface(Ci.nsIAsyncInputStream);
            // let output_stream = transport.openOutputStream(Cr.OPEN_UNBUFFERED | Cr.OPEN_BLOCKING, 0, 0);
            let output_stream = transport.openOutputStream(0, 0, 0);
            let connection = new Connection(socket, transport, input_stream, output_stream);
            let reader = new StreamReader(connection);
            input_stream.asyncWait(reader, 0, 0, main_thread);
        } catch (e) {
            print("ERROR: Command listener failed handling streams:", e.message);
            transport.close(Cr.NS_BINDING_ABORTED);
        }
    },
    onStopListening(socket, condition) {
        print("DEBUG: Socket listener stopped accepting connections. Status: 0x" + condition.toString(16));
        server_shutdown_done = true;
    }
};


function Connection(socket, transport, input, output) {
    this.socket = socket;
    this.transport = transport;
    this.input = input;
    this.output = output;
}

Connection.prototype = {
    reply: function (response) {
        print("DEBUG: Sending reply:", response);
        let cos = new ConverterOutputStream(this.output, "UTF-8", 8192, 0x0);
        try {
            // Protocol convention is to send one reply per line.
            cos.writeString(response + "\n");
            cos.flush();
            this.output.flush();
        } catch (e) {
            print("ERROR: Unable to send reply:", e.message);
            this.close();
        }
    },
    close: function () {
        print("DEBUG: Closing connection");
        try {
            this.output.flush();
            this.input.close();
            this.output.close();
        } catch (e) {
            print("ERROR: Unable to flush and close connection:", e.message)
        }
        this.transport.close(Cr.NS_OK);
    }
};


function StreamReader(connection) {
    this.connection = connection;
    this.buffer = "";
}

StreamReader.prototype = {
    onInputStreamReady: function (input_stream) {

        // First check if there is data available.
        // This check may fail when the peer has closed the connection.
        let data_available = false;
        try {
            data_available = input_stream.available() > 0;
        } catch (e) {
            if (e.message.indexOf("NS_BASE_STREAM_CLOSED") !== -1) {
                print("WARNING: Base stream was closed");
            } else {
                print("ERROR: Unable to check stream availability:", e.message);
            }
            if (this.buffer.length > 0)
                print("WARNING: Dropping non-empty buffer:", this.buffer);
            this.connection.close();
            return;
        }

        // An empty input stream means that the connection was closed.
        if (!data_available) {
            print("DEBUG: Connection closed by peer");
            if (this.buffer.length > 0)
                print("WARNING: Dropping non-empty buffer:", this.buffer);
            this.connection.close();
            return;
        }

        // Interpret available data as UTF-8 strings
        let cis = new ConverterInputStream(input_stream, "UTF-8", 0, 0x0);
        let str = {};
        try {
            cis.readString(4096, str);
        } catch (e) {
            print("ERROR: Unable to read input stream: ", e.message);
            if (this.buffer.length > 0)
                print("WARNING: Dropping non-empty buffer:", this.buffer);
            this.connection.close();
            return;
        }

        // When a read yields empty, the stream was likely closed.
        if (str.value.length === 0) {
            print("DEBUG: Empty read from stream. Assuming stream was closed");
            if (this.buffer.length > 0)
                print("WARNING: Dropping non-empty buffer:", this.buffer);
            this.connection.close();
            return;
        }
        this.buffer += str.value;

        // When there is data available, the protocol expects one request per line.
        if (this.buffer[this.buffer.length - 1] === '\n') {
            // The buffer we just read may have included several command lines
            this.buffer.slice(0, -1).split('\n').forEach((function (cmd_str) {
                print("DEBUG: handling request line:", cmd_str);
                handle_request(cmd_str, this.connection);
            }).bind(this));
            // Clear buffer for next incoming lines
            this.buffer = "";
        }

        // Must explicitly chain to receive more callbacks for this connection.
        input_stream.asyncWait(this, 0, 0, main_thread);

    }
};


/**
 * Global socket-based command server instantiation
 *
 * First and only argument to script is the optional command server port.
 * If no port is given, port 0 is used as default which means that the OS
 * chooses any free port.
 *
 * To signal success or failure, the first line written to stdout will be one
 * of the following:
 *
 *    "ERROR: Invalid port" -- Argument could not be parsed as integer value.
 *    "ERROR: Port in use"  -- Port is in use. Try a different one.
 *    "ERROR: Port refused" -- Port is privileged or there are no free ports
 *                             available.
 *    "ERROR: ..."          -- A different socket error occurred.
 *    "INFO: ... <PORT>"    -- Worker is now listening on port <PORT> and
 *                             switching to socket mode.
 */

// Parse port argument
let command_port = 0;
if (arguments.length > 0) command_port = parseInt(arguments[0], 10);
if (isNaN(command_port) || command_port < 0 || command_port > 65535) {
    print("ERROR: Invalid port");
    quit(10);
}

// Start the command server, listening locally on command port
var command_server;
try {
    command_server = new ServerSocket(command_port, true, 20);
} catch (e) {
    if (e.message.indexOf("(NS_ERROR_SOCKET_ADDRESS_IN_USE") !== -1) {
        print("ERROR: Port in use");
        quit(11);
    } else if (e.message.indexOf("(NS_ERROR_CONNECTION_REFUSED") !== -1) {
        print("ERROR: Port refused");
        quit(12);
    } else {
        print("ERROR:", e.message);
        quit(13);
    }
}
command_port = command_server.port;
command_server.asyncListen(SocketListener);
print("INFO: Listening on port", command_port);


/**
 * Main thread event handler loop
 */
let script_running = true;
let wakeup_pings = false;
let server_shutdown_done = false;

while (script_running) {
    print("DEBUG: Event loop");
    main_thread.processNextEvent(!wakeup_pings);
    if (wakeup_pings) {
        print("DEBUG: waiting for wakeup readline");
        readline();
    }
}


/**
 * Shutdown procedure
 */

print("DEBUG: Shutting down server");
command_server.close();

// TODO: close existing connections
print("DEBUG: Expect dangling connection callbacks to throw `print` type errors");

print("DEBUG: Handling remaining events");
while (!server_shutdown_done)
    while (main_thread.hasPendingEvents()) main_thread.processNextEvent(true);

quit(0);
