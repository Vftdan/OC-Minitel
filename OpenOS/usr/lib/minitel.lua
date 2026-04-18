local computer = require "computer"
local event = require "event"
local syssocket = require "sys.socket"
local net = {}
net.mtu = 8192
net.streamdelay = 30
net.minport = 32768
net.maxport = 65535
net.openports = {}

for k,v in pairs(computer.getDeviceInfo()) do
 if v.class == "network" then
  net.mtu = math.min(net.mtu, tonumber(v.capacity))
 end
end

function net.genPacketID()
 local npID = ""
 for i = 1, 16 do
  npID = npID .. string.char(math.random(32,126))
 end
 return npID
end

function net.usend(to,port,data,npID)
 computer.pushSignal("net_send",0,to,port,data,npID)
end

function net.rsend(to,port,data,block)
 local pid, stime = net.genPacketID(), computer.uptime() + net.streamdelay
 computer.pushSignal("net_send",1,to,port,data,pid)
 if block then return pid end
 repeat
  _,rpid = event.pull(0.5,"net_ack")
 until rpid == pid or computer.uptime() > stime
 if not rpid then return false end
 return true
end

-- ordered packet delivery, layer 4?
function net.send(to,port,ldata)
 local tdata, hsize = {}, 44 + #(os.getenv("HOSTNAME") or computer.address():sub(1,8)) + #to
 while hsize+#ldata > net.mtu do
  tdata[#tdata+1] = ldata:sub(1, net.mtu - hsize)
  ldata = ldata:sub(#tdata[#tdata]+1)
 end
 tdata[#tdata+1] = ldata
 for k,v in ipairs(tdata) do
  if not net.rsend(to,port,v) then return false end
 end
 return true
end

-- socket stuff, layer 5?

local hostname = computer.address():sub(1,8)
do
 local f=io.open("/etc/hostname","rb")
 if f then
  hostname = f:read()
  f:close()
 end
end

local loopbackListening = setmetatable({},{__mode="v"})
local listenerInfo = setmetatable({},{__mode="k"})

local function isLoopbackAddr(addr)
 return addr == hostname or addr == "localhost"
end

local function csend(self,data)
 if self.state == "open" then
  if not net.send(self.addr,self.port,data) then
   self:close()
   return false, "timed out"
  end
 end
end

local function socket(addr,port,sclose)
 local conn, recvfwd = syssocket.socketpair()
 conn.addr,conn.port = addr,tonumber(port)
 conn.send = csend
 conn.sclose = sclose
 local function listener(_,f,p,d)
  if f == conn.addr and p == conn.port then
   if d == sclose then
    recvfwd:close()
   else
    recvfwd:send(d)
   end
  end
 end
 event.listen("net_msg",listener)
 function conn.close(self)
  event.ignore("net_msg",listener)
  conn:shutdown()
  net.rsend(addr,port,sclose)
 end
 return conn
end

function net.open(to,port)
 if isLoopbackAddr(to) then
  local tbl = loopbackListening[port]
  if not tbl then
   return nil, "not listening"
  end
  local conn, reason = tbl.connect("a")
  if not conn then
   return nil, reason
  end
  conn.addr = to
  return conn
 end
 if not net.rsend(to,port,"openstream") then return false, "no ack from host" end
 local st = computer.uptime()+net.streamdelay
 local est = false
 while true do
  _,from,rport,data = event.pull(net.streamdelay, "net_msg")
  if to == from and rport == port then
   if tonumber(data) then
    est = true
   end
   break
  end
  if st < computer.uptime() then
   return nil, "timed out"
  end
 end
 if not est then
  return nil, "refused"
 end
 data = tonumber(data)
 sclose = ""
 repeat
  _,from,nport,sclose = event.pull("net_msg")
 until from == to and nport == data
 return socket(to,data,sclose)
end

local function registerListening(port,tbl,ssocket)
 loopbackListening[port] = tbl
 local function helper(_,from,rport,data)
  if rport == port and data == "openstream" then
   local nport = math.random(net.minport,net.maxport)
   local sclose = net.genPacketID()
   net.rsend(from,rport,tostring(nport))
   net.rsend(from,nport,sclose)
   tbl.upgrade(socket(from,nport,sclose))
  end
 end
 listenerInfo[helper] = {port,tbl,ssocket}
 event.listen("net_msg",helper)
 return helper
end

local function unregisterListening(helper)
 local info = listenerInfo[helper]
 if info then
  local port,tbl,ssocket = info[1],info[2]
  if loopbackListening[port] == tbl then
   loopbackListening[port] = nil
  end
  listenerInfo[helper] = nil
  tbl.close()
  if ssocket then
   ssocket:close()
  end
 end
 event.ignore("net_msg",helper)
end

function net.listen(port)
 local ssocket,tbl = syssocket.socketserver()
 local helper = registerListening(port,tbl,ssocket)
 local success,conn,reason = xpcall(ssocket.accept,debug.traceback,ssocket)
 unregisterListening(helper)
 ssocket:close()
 if not success then
  error(conn)
 end
 if not conn then
  error(reason)
 end
 if not conn.addr then
  conn.addr = "localhost"
 end
 return conn
end

function net.flisten(port,listener)
 local ssocket,tbl = syssocket.socketserver()
 local helper = registerListening(port,tbl,ssocket)
 ssocket.onConnect = function(conn)
  if not conn.addr then
   conn.addr = "localhost"
  end
  listener(conn)
 end
 ssocket:startDaemon()
 return helper
end

function net.ignore(helper)
 unregisterListening(helper)
end

return net
