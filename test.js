function d(strings) {
    let result = "";
    let counter = 0;
    let length = arguments.length - 1;
    for (let i  = 0; i < strings.length; i++) {
        result += strings[i];
        if (counter < length) {
            const argument = arguments[++counter];
            if (typeof argument === 'object') {
                result += JSON.stringify(argument);
            } else {
                result += argument;
            }
        }
    }
    return result;
}

function debug(message) {
    console.log(debug.caller.name + " " + message);
}

function foofun() {
    let foo = 1;
    let schnitzel = { mit: "sauce", borkdrive: 3000 };
    debug(d`Hello: ${foo} schnick ${schnitzel} schnak`);
}

foofun();
