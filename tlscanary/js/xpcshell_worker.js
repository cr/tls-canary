/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

"use strict";

const {classes: Cc, interfaces: Ci, utils: Cu, results: Cr} = Components;

const DEFAULT_TIMEOUT = 10000;

// This is a global random ID that is sent with every message to the Python world
const worker_id = Math.floor(Math.random() * 2**64);


Cu.import("resource://gre/modules/Services.jsm");
Cu.import("resource://gre/modules/XPCOMUtils.jsm");
Cu.import("resource://gre/modules/NetUtil.jsm");
Cu.import("resource://gre/modules/AppConstants.jsm");
Cu.importGlobalProperties(["XMLHttpRequest"]);

// generateQI was moved from XPCOMUtils to ChromeUtils in Fx 63
let generateQI = ChromeUtils.generateQI ? ChromeUtils.generateQI : XPCOMUtils.generateQI;
if (!generateQI) {
    print("WARNING: no valid generateQI found");
}

const nsINSSErrorsService = Ci.nsINSSErrorsService;
let nssErrorsService = Cc['@mozilla.org/nss_errors_service;1'].getService(nsINSSErrorsService);


function get_runtime_info() {
    const ac = JSON.parse(JSON.stringify(AppConstants));
    for (let key in ac) {
        if (key.endsWith("API_KEY")) {
            ac[key] = "__STRIPPED__";
        }
    }
    return {
        nssInfo: Cc["@mozilla.org/security/nssversion;1"].getService(Ci.nsINSSVersion),
        appConstants: ac
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
        if (value == undefined) type = "undefined";

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
        .createInstance(Ci.nsIFile);
    file.initWithPath(profile_path);
    let dir_service = Cc["@mozilla.org/file/directory_service;1"]
        .getService(Ci.nsIProperties);
    let provider = {
        getFile: function(prop, persistent) {
            persistent.value = true;
            if (prop == "ProfD" || prop == "ProfLD" || prop == "ProfDS" ||
                prop == "ProfLDS" || prop == "PrefD" || prop == "TmpD") {
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
    // This function copies and parses various properties of the connection state object
    // and wraps them into an info object to be returned with the command response.
    // Much of this is documented in https://developer.mozilla.org/en-US/docs/Web/API/
    // XMLHttpRequest/How_to_check_the_secruity_state_of_an_XMLHTTPRequest_over_SSL,
    // but that source has gone out of date with Firefox 63.

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
    info.security_state_status = false;
    info.security_state = null;
    info.ssl_status_status = false;
    info.ssl_status_errors = null;
    info.certified_usages = null;
    info.certificate_chain_length = null;
    info.certificate_chain = null;
    info.error_code = null;
    info.raw_error = null;
    info.short_error_message = null;

    // Try to query security info
    let sec_info = xhr.channel.securityInfo;
    if (sec_info == null)
        return info;

    // If sec_info is not null, it contains SSL state info
    info.security_info_status = true;

    // Ci.nsISSLStatusProvider was removed in Firefox 63 and
    // SSLStatus moved to Ci.nsITransportSecurityInfo, so only
    // query the interface if it exists.
    if (Ci.hasOwnProperty("nsISSLStatusProvider"))
        if (sec_info instanceof Ci.nsISSLStatusProvider) {
            sec_info.QueryInterface(Ci.nsISSLStatusProvider);
        }
    if (sec_info instanceof Ci.nsITransportSecurityInfo) {
        sec_info.QueryInterface(Ci.nsITransportSecurityInfo);
    }

    // At this point, sec_info should be decorated with either one of the following property sets:
    //
    // Fx 63+
    //   securityState, errorCode, errorCodeString, failedCertList, SSLStatus
    // Fx 52-62
    //   securityState, errorCode, errorMessage, failedCertList, SSLStatus

    // Process available SSL state and transfer to info object
    if (sec_info.securityState != null) {
        info.security_state_status = true;
        info.security_state = sec_info.securityState;
    } else {
        print("WARNING: securityInfo.securityState is null");
    }

    if (sec_info.SSLStatus != null) {
        info.ssl_status_status = true;
        info.ssl_status = sec_info.SSLStatus;
        // TODO: Find way to extract this py-side.
        try {
            let usages = {};
            let usages_string = {};
            info.ssl_status.server_cert.getUsagesString(true, usages, usages_string);
            info.certified_usages = usages_string.value;
        } catch (e) {
            info.certified_usages = null;
        }
    } else {
        // Warning is too noisy
        // print("WARNING: securityInfo.SSLStatus is null");
    }

    // Process errorCodeString or errorMessage
    if (sec_info.hasOwnProperty("errorMessage")) {
        // Old message format wich needs to be parsed
        info.raw_error = sec_info.errorMessage;
        if (info.raw_error) {
            try {
                info.short_error_message = info.raw_error.split("Error code:")[1].split(">")[1].split("<")[0];
            } catch (e) {
                print("WARNING: unexpected errorMessage format: " + e.toString());
                info.short_error_message = info.raw_error;
            }
        } else {
            info.raw_error = null;
            info.short_error_message = null;
        }
    } else if (sec_info.hasOwnProperty("errorCodeString")) {
        info.raw_error = sec_info.errorCodeString;
        info.short_error_message = sec_info.errorCodeString;
    } else {
        print("WARNING: securityInfo has neither errorCodeString nor errorMessage");
    }
    if (sec_info.hasOwnProperty("errorCode")) {
        info.error_code = sec_info.errorCode;
    } else {
        print("WARNING: securityInfo has no errorCode");
    }

    // Extract certificate objects if requested
    if (info.ssl_status_status && report_certs) {
        let server_cert = info.ssl_status.serverCert;
        let cert_chain = [];
        if (server_cert.sha1Fingerprint) {
            cert_chain.push(server_cert.getRawDER({}));
            let chain = [];
            if (info.ssl_status.succeededCertChain != null) {
                chain = info.ssl_status.succeededCertChain;
            } else if (info.ssl_status.failedCertChain != null) {
                chain = info.ssl_status.failedCertChain;
            }
            let enumerator = chain.getEnumerator();

            // XPCOMUtils.IterSimpleEnumerator removed in Firefox 63 (bug 1484496)
            let cert_enumerator = XPCOMUtils.IterSimpleEnumerator ?
                XPCOMUtils.IterSimpleEnumerator(enumerator, Ci.nsIX509Cert) : enumerator;
            for (let cert of cert_enumerator) {
                cert_chain.push(cert);
            }
        }
        info.certificate_chain_length = cert_chain.length;
        info.certificate_chain = cert_chain;
    }

    // Some values might be missing from the connection state, for example due
    // to a broken SSL handshake. Try to catch exceptions before report_result's
    // JSON serializing does.
    if (info.ssl_status_status) {
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
        QueryInterface: generateQI([Ci.nsIChannelEventSink])
    };

    let request = new XMLHttpRequest();
    try {
        request.mozBackgroundRequest = true;
        request.open("HEAD", "https://" + host, true);
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
        response_cb(false, {origin: "request_error", error: error, info: collect_request_info(request, false)});
    }
}


// Command object definition. Must be in-sync with Python world.
// This is used for keeping state throughout async command handling.
function Command(json_string) {
    let parsed_command = JSON.parse(json_string);
    this.id = parsed_command.id ? parsed_command.id : Math.floor(Math.random() * 2**64);
    this.mode = parsed_command.mode;
    this.args = parsed_command.args;
    this.original_cmd = parsed_command;
    this.start_time = new Date();
}

// Even though it's a prototype method it will require bind when passed as callback.
Command.prototype.send_response = function _report_result(success, result) {
    // Send a response back to the python world
    print(JSON.stringify({
        "id": this.id,
        "worker_id": worker_id,
        "original_cmd": this.original_cmd,
        "success": success,
        "result": result,
        "command_time": this.start_time.getTime(),
        "response_time": new Date().getTime(),
    }));
};

Command.prototype.handle = function _handle() {
    switch (this.mode) {
        case "info":
            this.send_response(true, get_runtime_info());
            break;
        case "useprofile":
            set_profile(this.args.path);
            this.send_response(true, "ACK");
            break;
        case "setprefs":
            set_prefs(this.args.prefs);
            this.send_response(true, "ACK");
            break;
        case "scan":
            // .bind is required for callback to avoid
            // 'this is undefined' when called from request handlers.
            scan_host(this.args, this.send_response.bind(this));
            this.send_response(true, "ACK");
            break;
        case "quit":
            script_done = true;
            // Intentional fall-through
        case "wakeup":
            while (main_thread.hasPendingEvents()) main_thread.processNextEvent(true);
            this.send_response(true, "ACK");
            break;
        default:
            this.send_response(false, "Unknown command mode: " + this.mode);
    }
};


/**
 * Glue code between socket server requests and TLS Canary functions.
 */

function handle_request(request, connection) {
    // print("DEBUG: Handling request:", request);
    try {
        const cmd = new Command(request, connection);
        cmd.handle();
    } catch (error) {
        print("ERROR: Unable to handle command:", error.toString());
    }
}

function send_reply(reply_string, connection) {
    connection.reply(reply_string);
}


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
let command_server;
try {
    command_server = CreateServerSocket(command_port, true, 20);
} catch (error) {
    if (error.result === Cr.NS_ERROR_SOCKET_ADDRESS_IN_USE) {
        print("ERROR: Port in use");
        quit(11);
    } else if (error.result === Cr.NS_ERROR_CONNECTION_REFUSED) {
        print("ERROR: Port refused");
        quit(12);
    } else {
        print("ERROR:", error.toString());
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
print("DEBUG: Expect dangling connection callbacks to throw `print` or `Cr` errors");

print("DEBUG: Handling remaining events");
while (!server_shutdown_done)
    while (main_thread.hasPendingEvents()) main_thread.processNextEvent(true);

quit(0);




//
//
// // The main loop only reads and handles commands from stdin.
//
// let script_done = false;
// let thread_manager = Cc["@mozilla.org/thread-manager;1"].getService(Ci.nsIThreadManager);
// let main_thread = thread_manager.mainThread;
//
// while (!script_done) {
//     let cmd = null;
//     try {
//         cmd = new Command(readline());
//         cmd.handle();
//     } catch (error) {
//         print(error);
//     }
// }
