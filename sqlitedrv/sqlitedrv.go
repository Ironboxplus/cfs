// Package sqlitedrv registers the pure-Go SQLite driver (database/sql name
// "sqlite") that the volume index needs. Standalone programs and tests import
// it; a program that already registers a driver of that name (OpenList uses
// glebarez/sqlite) must not, or database/sql panics on the second Register.
package sqlitedrv

import _ "modernc.org/sqlite"
