
-- seems already supported by luasql (but not documented for sqlite3)
local function luasql_sqlite3_con_getlastautoid_support(con)
	if not con.getlastautoid then
		con.getlastautoid = function()
			return con:execute('SELECT last_insert_rowid();')
		end
		return true
	end
	return false
end


