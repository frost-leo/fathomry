function setAttribute(n, v, dispatchEvents) {
    this[n] = v;
    if (n === 'value' && dispatchEvents !== false) {
        this.dispatchEvent(new Event('input', {bubbles: true}));
        this.dispatchEvent(new Event('change', {bubbles: true}));
    }
    return this[n];
}
