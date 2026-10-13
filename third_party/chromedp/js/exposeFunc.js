function exposeFunc(name, binding) {
    var scope = globalThis;
    var reply = binding + '_reply';
    var revoke = binding + '_revoke';
    var send = scope[binding];
    if (Object.prototype.hasOwnProperty.call(scope, reply) || typeof send !== 'function') {
        return;
    }
    var previous = Object.getOwnPropertyDescriptor(scope, name);
    var pending = new Map();
    var last = 0;
    var closed = false;
    var fn = function () {
        if (closed) {
            return Promise.reject(new Error('binding closed'));
        }
        var args = Array.prototype.slice.call(arguments);
        return new Promise(function (resolve, reject) {
            var id = ++last;
            pending.set(id, {resolve: resolve, reject: reject});
            try {
                send(JSON.stringify({id: id, args: args}));
            } catch (e) {
                pending.delete(id);
                reject(e);
            }
        });
    };
    var settle = function (id, ok, value) {
        var call = pending.get(id);
        if (closed || !call) {
            return false;
        }
        pending.delete(id);
        if (ok) {
            call.resolve(value);
        } else {
            call.reject(new Error(value));
        }
        return true;
    };
    var stop = function () {
        closed = true;
        pending.forEach(function (call) { call.reject(new Error('binding closed')); });
        pending.clear();
        var removed = true;
        var current = Object.getOwnPropertyDescriptor(scope, name);
        if (current && current.value === fn) {
            if (previous) {
                Object.defineProperty(scope, name, previous);
            } else {
                removed = delete scope[name] && removed;
            }
        }
        if (scope[reply] === settle) { removed = delete scope[reply] && removed; }
        if (scope[binding] === send) { removed = delete scope[binding] && removed; }
        if (removed && scope[revoke] === stop) { removed = delete scope[revoke]; }
        return removed;
    };
    // Install cleanup first, so a later property failure remains reversible.
    Object.defineProperty(scope, revoke, {value: stop, configurable: true});
    Object.defineProperty(scope, name, {
        value: fn,
        writable: true,
        configurable: true,
        enumerable: true,
    });
    Object.defineProperty(scope, reply, {value: settle, configurable: true});
}
