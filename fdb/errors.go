package fdb

// SOMEDAY: these (along with others) should be coming from fdb.options?
const (
	errorClientInvalidOperation = 2000
	errorNetworkNotSetup = 2008

	errorApiVersionUnset = 2200
	errorApiVersionAlreadySet = 2201
	errorApiVersionInvalid = 2202
	errorApiVersionNotSupported = 2203
)
