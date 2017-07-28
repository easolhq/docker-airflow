const argv = require('minimist')(process.argv.slice(2));
const cryptobject = require('cryptobject');

/**
 * Lookup the function for a given command name.
 * @param {string} name - Command name representing a function.
 * @return {function} The function to be run.
 */
function lookupCommandFunc(name) {
  // func = the js function to run
  if (name === 'encrypt') {
    func = cryptobject.encrypt;
  } else if (name === 'decrypt') {
    func = cryptobject.decrypt;
  } else {
    console.log('ERROR: command "' + name + '" not found');
    process.exit(1);
  }
  return func;
}

/**
 * Apply the desired encryption function, JSONifying from stdin and to stdout.
 */
function apply() {
  func = lookupCommandFunc(argv.command);
  process.stdout.write(
    JSON.stringify(
      func(argv.passphrase, JSON.parse(argv.obj))
    )
  );
}

apply();
