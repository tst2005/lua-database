--[[--------------------------------------------------------
	-- Database - A Database Asbraction Layer for Lua --
	-- Copyright (c) 2014-2015 TsT worldmaster.fr --
--]]--------------------------------------------------------

-- a voir :
-- https://github.com/FPtje/MySQLite/blob/master/mysqlite.lua
-- https://github.com/esmil/lem-sqlite3/blob/master/lem/sqlite3/queued.lua
-- https://github.com/moteus/lua-sqlite3/blob/master/sqlite3.lua

local sqlite3 = require "luasql.sqlite3"
if type(sqlite3) == "table" and type(sqlite3.sqlite3) == "function" then -- I dunno why the sqlite3 module is inside the "luasql.sqlite3" module
	sqlite3 = sqlite3.sqlite3
end

local databaseschema = require("databaseschema")
local schemaToSql = assert(databaseschema.schemaToSql)
local schemaIsValid = assert(databaseschema.schemaIsValid)
local schemaTables = assert(databaseschema.schemaTables)

local cc2 = require("classcommons2")
local class, instance = assert(cc2.class), assert(cc2.instance)

local _M = {}
_M.printdebug = function() end

local function _init(self)
	self.printdebug = _M.printdebug
--	self:printdebug("dbClass init()")
--	self:newenv()
end
local mtClass = {}
mtClass.init = assert(_init)
local dbClass = class("dbClass", assert(mtClass) )
-- 30log specific printing feature (see: https://github.com/Yonaba/30log/#printing-classes-and-objects )

local function new()
	return instance(dbClass)
end

function dbClass:newenv()
--	self:printdebug("dbClass env new()")
	local env = assert(sqlite3())
	self.env = env
	return
end

function dbClass:setSchema(schema)
	local valid, reason = schemaIsValid(schema)
	if not valid then
		error(reason, 2)
	end

	local schemas = self.schema or {}
	for k,v in pairs(schema) do
		schemas[k] = v
	end
	self.schema = schemas
	return
end

-- db:unsetSchema(schema)
-- schema: can be key-table or itable
-- schema {['t1']=true, 't2'} forgot tables t1 and t2
function dbClass:unsetSchema(schema)
	local schemas = self.schema or {}
	for k,v in pairs(schema) do
		if type(k) == "number" then
			if schemas[v] then
				schemas[v] = nil
			end
		elseif schemas[k] then
			schemas[k] = nil
		end
	end
        self.schema = schemas
	return
end

-- expose the schema
function dbClass:getSchema()
	return self.schema
end


function dbClass:schemaTables()
	return schemaTables(self.schema)
end


-- setBackend <dbtype>
-- valid dbtypes : 'mysql'|'sqlite'
function dbClass:setBackend(dbtype)
	self.dbtype = dbtype
	return self
end

--dbfile = "/tmp/luasql-test"
--dbfile = "" -- temporary (no file created)
--dbfile = ":memory:" -- in memory (no file created)

function dbClass:open(dbfile)
	local dbfile = dbfile or ""

	if not self.env then self:newenv() end
	local env = assert(self.env)

	assert(self.con == nil, "DB seems already opened")
	local con
	local ok, err = pcall( function() con = assert(env:connect(dbfile)) end )
	if not ok then
		error("Fail to open database "..tostring(dbfile).."\nenv:connect() error message: "..err, 2)
	end
	self.con = con
	self:printdebug("dbClass con new()")

	self.dbfile = dbfile

	con:setautocommit(true)
	return self
end

function dbClass:close()
	self:printdebug("db:close()")
	if self.curs then
		for cur in pairs(self.curs) do
			--self:printdebug("dbClass curs close():"..tostring(cur))
			if cur ~= "n" then
				cur:close()
				self.curs[cur] = nil
				self.curs.n = self.curs.n -1
			end
		end
	end
	if self.con then
		self.con:close()
		self.con = nil
		--self:printdebug("dbClass con close()")
	end
	if self.env then
		self.env:close()
		self.env = nil
		--self:printdebug("dbClass env close()")
	end
end

local function assertcon(con, msg)
	if con == nil then
		error(msg or "DB is not open. Use db:open() first.", 3)
	end
	return con
end

local function handleforvalue(value)
	local fetch = function() return 1, value end
	return setmetatable({
                fetch = fetch,
                fetchall = function() return {value} end,
                close = function() end,
        }, {    __call = fetch, }) --TODO: __tostring
end

local function execute(self, sql_statement)
	local conn = assertcon(self.con)
	-- retrieve a cursor
	local cursor = conn:execute(sql_statement)
	if type(cursor) == "number" or type(cursor) == "string" or type(cursor) == "nil" then
		return handleforvalue(cursor)
	end
	local cursors = self.curs or {}
	self.curs = cursors
	cursors[cursor] = true
	cursors.n = (cursors.n or 0) +1

	local close = function()
		if cursors[cursor] then
			self:printdebug("cursor:close()")
			cursor:close()
			cursors[cursor] = nil
			cursors.n = cursors.n -1
		end
	end
	local function closeifnotresult(...)
		if #{...} == 0 then
			close()
		end
		return ...
	end
	local fetch = function(_self_, tab, mode)
		--local tab  = tab == nil and {} or tab    -- default with tab
		--local mode = mode == nil and "a" or mode -- default key index
		return closeifnotresult(cursor:fetch(tab, mode)) -- cur:fetch({}, "a")
	end
	local fetchall = function(_self_, tab, mode)
		local mode = mode == nil and "a" or mode -- default key index
		local tab = tab or {}
		while true do
			--local ok, row = pcall(cursor.fetch, cursor, {}, mode)
			--if not ok then
			--	close()
			--	self:printdebug("break coz error:", row)
			--	break
			--end
			local row = closeifnotresult(cursor:fetch({}, mode))
			if not row then break end
			tab[#tab+1] = row
		end
		close()
		return tab, mode
	end
	local fetchvalues = function(_self_, tab, mode)
		--local mode = mode == nil and "a" or mode -- default key index
		local tab = tab or {}
		local tab = { closeifnotresult(cursor:fetch()) }
		close()
		return tab
	end

	return setmetatable({
		fetch = fetch,
		fetchall = fetchall,
		fetchvalues = fetchvalues,
		close = close,
	}, {	__call = fetch, }) --TODO: __tostring
end

function dbClass:cursors()
	return self.curs and self.curs.n or 0
end

function dbClass:pragmaQuery(sqlpart)
	assertcon(self.con)

	local sql = ("PRAGMA %s;"):format(sqlpart)
	local hand = execute(self, sql)
	--print("dbClass:pragmaQuery(sqlpart)="..sqlpart)

	local res = hand:fetchall()
	--print("res=",res, "#res=", #res)

	for k, v in pairs(res) do
		if type(v) == "table" then
			for k2,v2 in pairs(v) do
				print(k, k2, v2)
			end
		else
			print(k, tostring(v))
		end
	end

	local cur = nil
--[[	if type(cur) == "number" or type(cur) == "string" or cur == nil then
		print("cur=", cur)
		return true, nil, nil
	end
	local res
	if cur and cur.fetch then
		ret = cur:fetch({}, "a")
		if res ~= "ok" then
			return false, res, cur
		end
	end
]]--
	return true, res, cur
end



-- https://www.sqlite.org/pragma.html#pragma_query_only
-- PRAGMA query_only;
-- PRAGMA query_only = boolean;

-- WARNING: sqlite3 seems buggy, the query_only seems not available at all!
-- WORKAROUND: we should emul it softly ?
function dbClass:readonly(ro)
	if ro == nil then
		-- ask the DB ? PRAGMA query_only; ?
		return self.readonly -- return the current state
	end
	if type(ro)~="boolean" then
		error("bad argument #1 to 'readonly' (boolean expected, got "..type(ro)..")", 2)
	end
	local ok, err, cur = self:pragmaQuery("query_only = "..tostring(ro))
	if ok then
		self.readonly = ro
	end
	if cur then cur:close() end
	return ok, err
end

-- https://www.sqlite.org/pragma.html#pragma_quick_check
-- PRAGMA quick_check;
-- PRAGMA quick_check(N)

-- https://www.sqlite.org/pragma.html#pragma_integrity_check
-- PRAGMA integrity_check; (default N=100)
-- PRAGMA integrity_check(N) (if error, N first error returns else return string'ok')

function dbClass:dbcheck(fast, N)
	if fast == nil then fast = false end
	if type(fast) ~= "boolean" then
		error("bad argument #1 to 'dbcheck' (boolean or nil expected, got "..type(fast)..")", 2)
	end

	local sqlpart =
		(fast and "quick_check" or "integrity_check")..
		(N and "("..N..")" or '')

	local ok, res, cur = self:pragmaQuery(sqlpart)
	if not ok then
		print("DB[sqlite] dbcheck got errors:")
		if type(res) == "table" then
			for k,e in pairs(res) do
				print(k, e) -- print errors
			end
		else
			print(tostring(res))
		end
		print("DB[sqlite] end of dbcheck")
	end
	if cur then cur:close() end
	return ok
end

function dbClass:reset()
	local dbtype, schema = self.dbtype, self.schema
	assert(dbtype, "'dbtype' is not set. Use db:setBackend() first.")
	assert(schema, "schema is not set. Use db:setSchema() first.")
	assertcon(self.con)

	for tablename in pairs(schema) do
		self:drop(tablename)
	end

	for tablename in pairs(schema) do
		self:schemaCreate(tablename)
	end

	return
end

function dbClass:schemaCreate()
        local dbtype, schema = self.dbtype, self.schema
        assert(dbtype, "'dbtype' is not set. Use db:setBackend() first.")
        assert(schema, "schema is not set. Use db:setSchema() first.")
        assertcon(self.con)

	local initSql = schemaToSql(schema, dbtype)
	for i,sql in ipairs(initSql) do
--		self:printdebug("---------------------------------")
		self:printdebug(sql)
		local hand = execute(self, sql)
		hand:close()
--		self:printdebug("initSql done, returns "..tostring(res))
	end
--	self:printdebug("---------------------------------")
end

function dbClass:escape(data)
	-- data:gsub("'", "''") ?
	local con = assertcon(self.con)
	return con:escape(data)
end

function dbClass:setautocommit(value)
	assert(type(value) == "boolean", "bad argument #2 to 'install' (table expected, got "..type(value)..")")
	local con = assertcon(self.con)
	con:setautocommit(not not value)
	return
end

function dbClass:begin()
	local con = assertcon(self.con)
	local hand = execute(self, "BEGIN TRANSACTION;")
	hand:close()
end
function dbClass:commit()
	local con = assertcon(self.con)
	local hand = execute(self, "COMMIT TRANSACTION;")
	hand:close()
end

function dbClass:transation(func, ...)
	local commitreturn = function(...)
		self:commit()
		return ...
	end
	self:begin()
	return commitreturn(func(...))
end


function dbClass:create(name, columns)

end

function dbClass:drop(name)
	local dbtype, schema, con = self.dbtype, self.schema, assertcon(self.con)
	assert(dbtype, "'dbtype' is not set. Use db:setBackend() first.")
	assert(schema, "schema is not set. Use db:setSchema() first.")

	local dropsql = ("DROP TABLE IF EXISTS %s;"):format(name) -- should be generated from shema layer (with dbtype)?
	self:printdebug(dropsql)
	local hand = execute(self, dropsql)
	return hand:fetchall()
end


function dbClass:inserts(name, t_rows)
	for i,row in ipairs(t_rows) do
		local res = self:insert(name, row)
		assert(res == nil or res == 0)
	end
end

-- :insert <(string)SQL statement>		raw sql insert
-- :insert <key-value table>			insert value by column+value way
-- :insert <value table>			insert values in order
--
function dbClass:insert(name, row)
	assert(type(row) == "table")
	local con = assertcon(self.con)

	local sql
	if type(row) == "table" then
		local protect = function(fieldvalue)
			if type(fieldvalue) == "table" and type(fieldvalue.raw) == "string" then -- the way to use an unquoted string
				-- fieldvalue={raw="SQL STUFF"}
				return fieldvalue.raw
			elseif type(fieldvalue) == "string" then
				return "'"..con:escape(fieldvalue).."'" -- lua string, auto-quoted
			elseif type(fieldvalue) == "number" then
				return tostring(fieldvalue) -- lua number
			end
			error("WARNING: field "..fieldname.." have invalid data type ("..type(fieldvalue).."). Allowed type are string/number/table with 'raw' key.")
			return tostring(fieldvalue) -- not a string, not a number, !?!
		end
		if #row == 0 then -- key/value
			local fieldnames = {}
			local fieldvalues = {}
			for fieldname, fieldvalue in pairs(row) do
				fieldnames[#fieldnames+1] = con:escape(fieldname)
				fieldvalues[#fieldvalues+1] = protect(fieldvalue)
			end
			sql = ("INSERT INTO %s(%s) VALUES(%s);"):format(name, table.concat(fieldnames, ", "), table.concat(fieldvalues, ", "))
		else
			local fieldvalues = {}
			for i,fieldvalue in ipairs(row) do
				fieldvalues[#fieldvalues+1] = protect(fieldvalue)
			end
			sql = ("INSERT INTO %s VALUES(%s);"):format(name, table.concat(fieldvalues, ", "))
		end
	elseif type(row) == "string" then
		sql = row
	else
		error("dbClass:insert(): invalid format for row: "..type(row))
	end
--	self:printdebug(sql)
	local hand = execute(self, sql)
	local ret = table.concat(hand:fetchall(nil, "n"), ";")
--	self:printdebug("returns "..tostring(ret))
	return ret
end


-- Challenge: be able to cur:close() at the end of data (and how to close before the end of data when the application quit)
-- 1) luasql for sqlite does not support cursor.numrows()
--	if cursor.numrows then
--		local n = cursor:numrows()
--		-- check if cursor still have data, if not, con:close()
--		if n == 0 then
--			print("cursor (closed?)", n)
--		else
--			print("cursor", n)
--		end
--	end
-- 2) close the cursor at runtime when no more data is got (when the result is empty, no argument at all)
-- Problem: if the result is only 1 value, the coder will usually forgot to close the handler...
-- 3) solution: index all cursors, and auto close when no more result, force close with result:close() or close all cursors on db:close()
--	result(...)
--	result:fetch(...)
--	result:fetchall()
--	result:close()

-- db:select <name>, <w>, [<action>]
-- <name> : the table name
-- <w>    : a table containing :
--          - columns = "field1,field2,..."|"*"
--          - where = "sql-where-close"
--          - limit = "sql-limit-close"
--          - orderby = "sql-orderby-close"
-- <action>: not-used-yet
-- db:select("t1", {columns="x,y,z", where="x == 'titi'", limit="1", orderby="x ASC",}, nil)
function dbClass:select(name, w, action)
	local dbtype, schema, con = self.dbtype, self.schema, assertcon(self.con)
	assert(dbtype, "'dbtype' is not set. Use db:setBackend() first.")
	assert(schema, "schema is not set. Use db:setSchema() first.")

	local columns, where, limit, orderby
	if type(w) == "string" then
		columns = w
	elseif type(w) == "table" then
		columns = w.columns
		columns = (type(columns) == "table" and table.concat(columns, ", ")) or columns
		assert(columns, "bad argument #2 to 'select' (missing mandatory 'columns' field in table)")

		where = w.where
		where = where and ' WHERE '..where

		limit = w.limit
		limit = limit and ' LIMIT '..limit

		orderby = w.orderby
		orderby = orderby and ' ORDER BY '..orderby
	else
		error("bad argument #2 to 'select' (string or table expected, got "..type(w)..")", 2)
	end

	local sql = ("SELECT %s FROM %s%s%s%s;"):format(columns or '*', name, where or '', orderby or '', limit or '')
	self:printdebug(sql)
	return execute(self, sql)
end


-- :query() -- table/value result ?
-- :queryvalue() -- 1 value
-- :haveoneresult() -- error if more than one result ?


-- :lasterror ? --
-- :queryraw(...) --
-- SQLStr -- alias for escape
-- TableExists -- alias for exists
-- compat with https://maurits.tv/data/garrysmod/wiki/wiki.garrysmod.com/index27a7.html


function dbClass:update(name, set, action)

end

function dbClass:delete(name, action)

end

-- FIXME: not working correctly
--[=[
function dbClass:getColumns(table_name)
	local con = assertcon(self.con)

	local cur = con:execute( ("SELECT sql FROM sqlite_master WHERE tbl_name = %s AND type = 'table'"):format( con:escape(table_name) ) )
	print(cur)

--	local cur = con:execute( ("PRAGMA table_info(%s);"):format( con:escape(table_name) ) )

--	local row = cur:fetch({}, "a")
--	while row do
--		print(string.format("Name: %s, E-mail: %s", row.name, row.email))
--		-- reusing the table of results
--		row = cur:fetch (row, "a")
--	end

	--local row = cur:fetch({}, "a")
	for k,v in pairs( cur:getcolnames() ) do
		print(k,v)
	end
--	while row do
--		for k,v in pairs(row) do
--			print(k,v)
--		end
--		row = cur:fetch(row, "a")
--	end
	cur:close()
	return
end
]=]--

function dbClass:getRows(name)
	--return #self.data[name][1]
end
function dbClass:exists(name) -- table exists ? column exists ? entry exists ?
	--return self.data[name] ~= nil
end
function dbClass:query(query)

end

_M.new = new
return _M
