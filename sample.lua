--[[--------------------------------------------------------
	-- Database - A Database Asbraction Layer for Lua --
	-- Copyright (c) 2014-2015 TsT worldmaster.fr --
--]]--------------------------------------------------------

-- original from : https://github.com/TTheu/LuaSqlite3/blob/master/BasicCRUD.lua

local backend = require 'database.sqlite3'
local dbschema = require 'databaseschema.databaseschema'

local db = backend.new()

db:open('game.db')
db:setBackend('sqlite')
db:setSchema({
	['players'] = {
		['fields'] = {
			name = { type = "text", },  -- Note: string is invalid sqlite type
			class = { type = "text", }, -- Note: string is invalid sqlite type
			lastlogin = { type = "datetime", },
			created = { type = "datetime", },
		},
	}
})

local function createTablePlayers()
	db:schemaCreate('players')
end

local function insertPlayers(list)
	for i, p in pairs(list) do
		local hand = db:select("players", {columns='*', where=('name = "%s"'):format(db:escape(p.name)),})
		local res = hand:fetchall(nil, "n")
		if #res == 0 then
			db:insert("players", p)
		else
			print('Player já cadastrado!')
		end
	end
end

local function listPlayers()
	local hand = db:select("players", {columns='*'})
	for i,row in ipairs(hand:fetchall()) do
		print('\n---- New Line ----\n')
		for k,v in pairs(row) do
			print(k,v)
		end
	end
end

-- Call function to Create Table "Players"
createTablePlayers()

-- Call function to Insert Player(s)
local list = {
	{name='Max', class='Knight', lastlogin=0, created=os.date('%c')},
}
insertPlayers(list)

-- Call function to List Player(s)
listPlayers()

db:close()
