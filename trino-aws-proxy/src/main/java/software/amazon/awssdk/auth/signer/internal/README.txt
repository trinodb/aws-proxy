Ad hoc testing shows that some clients are including the `user-agent`
header in the signature. The AWS signing code explicitly ignores this
header and, thus, these signatures don't match. The set of headers
to ignore is embedded in a private static final set and can't be
accessed or altered. Therefore, make copies of the auth code, mark
it as "copied", check for the presence of the `user-agent` signature
header and use this copied/legacy code in this case. This is legacy
code and, so, will never change so this is a safe method. Future
code changes would only affect non-legacy signatures.