local modulename = "abtestingDiversionUGroup"

local _M    = {}
local mt    = { __index = _M }
_M._VERSION = "0.0.1"

local ERRORINFO	= require('abtesting.error.errcode').info

-- local k_ugroup     = 'ugroup'
local k_ugroup  = 'ugroup'
local k_upstream= 'upstream'
local k_userinfo = 'ab:userinfo:'
local default_group = 'stable'

_M.new = function(self, database, policyLib)
    if not database then
        error{ERRORINFO.PARAMETER_NONE, 'need avaliable redis db'}
    end if not policyLib then
        error{ERRORINFO.PARAMETER_NONE, 'need avaliable policy lib'}
    end
    
    self.database = database
    self.policyLib = policyLib
    return setmetatable(self, mt)
end

local isNULL = function(v)
    return v and v ~= ngx.null
end

--	policy is in format as {{upstream = 'beta', ugroup ={ '1','2'} }, {}}
_M.check = function(self, policy)
    for _, v in pairs(policy) do
        local ugroup    = v[k_ugroup]
        local upstream  = v[k_upstream]
        
        local v_ugroup    = ugroup 
        local v_upstream  = upstream and upstream ~= ngx.null
        
        if not v_ugroup or not v_upstream then
            local info = ERRORINFO.POLICY_INVALID_ERROR 
            local desc = ' k_ugroup or k_upstream error'
            return {false, info, desc}
        end
        
        --TODO: need to check upstream alive
    end
    
    return {true}
end

--	policyData will be in hash table  
_M.set = function(self, policy)
    local database  = self.database 
    local policyLib = self.policyLib
    
    database:init_pipeline()
    for _, v in pairs(policy) do
        local ugroup   = v[k_ugroup]
        local upstream = v[k_upstream] 
        database:hset(policyLib, ugroup, upstream)
    end
    local ok, err = database:commit_pipeline()
    if not ok then 
        error{ERRORINFO.REDIS_ERROR, err} 
    end
end

_M.get = function(self)
    local database  = self.database 
    local policyLib = self.policyLib
    
    local data, err = database:hgetall(policyLib)
    if not data then 
        error{ERRORINFO.REDIS_ERROR, err} 
    end

    return data
end

_M.getUpstream = function(self, abtoken)

    if not abtoken then 
	log.info("not abtoken")
	return default_group
    end   
    
--    ngx.log(ngx.INFO,"getupstream .............................")
 
    local database, key = self.database, self.policyLib
   
    -- 读取redis中当前用户的group,group的值即为backend
    local backend, err = database:hget(k_userinfo..abtoken , "group" ) 
    
    if backend == ngx.null then backend = nil end
    
    return backend
end

return _M
