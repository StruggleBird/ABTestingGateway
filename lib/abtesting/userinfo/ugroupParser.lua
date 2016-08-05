local _M = {
    _VERSION = '0.01'
}

_M.get = function()
        local u = ngx.var.cookie_ab_token
        return u
end
return _M
