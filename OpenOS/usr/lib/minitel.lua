local computer = require "computer"
local event = require "event"
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

local loopbackAccepted = setmetatable({},{__mode="v"})

local function isLoopbackAddr(addr)
 return addr == hostname or addr == "localhost"
end

local function cwrite(self,data)
 if self.state == "open" then
  if not net.send(self.addr,self.port,data) then
   self:close()
   return false, "timed out"
  end
 end
end
local function fdxwrite(self,data)
 if self.state == "open" then
  self.other.rbuffer = self.other.rbuffer .. data
  computer.pushSignal("loopback_write")  -- wake the reading thread up
 end
end
local function cread(self,length)
 length = length or "\n"
 local rdata = ""
 if type(length) == "number" then
  rdata = self.rbuffer:sub(1,length)
  self.rbuffer = self.rbuffer:sub(length+1)
  return rdata
 elseif type(length) == "string" then
  if length:sub(1,2) == "*a" then
   rdata = self.rbuffer
   self.rbuffer = ""
   return rdata
  elseif length:len() == 1 then
   local pre, post = self.rbuffer:match("(.-)"..length.."(.*)")
   if pre and post then
    self.rbuffer = post
    return pre
   end
   return nil
  end
 end
end
local function fdxclose(self)
 self.state = "closed"
 self.other.state = "closed"
end

local function socket(addr,port,sclose)
 local conn = {}
 conn.addr,conn.port = addr,tonumber(port)
 conn.rbuffer = ""
 conn.write = cwrite
 conn.read = cread
 conn.state = "open"
 conn.sclose = sclose
 local function listener(_,f,p,d)
  if f == conn.addr and p == conn.port then
   if d == sclose then
    conn:close()
   else
    conn.rbuffer = conn.rbuffer .. d
   end
  end
 end
 event.listen("net_msg",listener)
 function conn.close(self)
  event.ignore("net_msg",listener)
  conn.state = "closed"
  net.rsend(addr,port,sclose)
 end
 return conn
end

local function socketpair()
 local up, down = {}, {}
 up.other = down
 down.other = up
 for _, conn in ipairs{up, down} do
  conn.rbuffer = ""
  conn.write = fdxwrite
  conn.read = cread
  conn.state = "open"
  conn.close = fdxclose
 end
 return up, down
end

function net.open(to,port)
 if isLoopbackAddr(to) then
  local st = computer.uptime()+net.streamdelay
  computer.pushSignal("loopback_connect",to,port)
  while true do
   for i, conn in ipairs(loopbackAccepted[port] or {}) do
    if conn.addr == to then
     return table.remove(loopbackAccepted[port], i)
    end
   end
   event.pull()
   if st < computer.uptime() then
    return nil, "timed out"
   end
  end
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

function net.listen(port)
 local loopbackQueue = loopbackAccepted[port] or {}
 loopbackAccepted[port] = loopbackQueue
 local e, from, rport, data
 repeat
  e, from, rport, data = event.pullMultiple("net_msg", "loopback_connect")
 until rport == port and (e == "net_msg" and data == "openstream" or e == "loopback_connect")
 if e == "loopback_connect" then
  local connServer, connClient = socketpair()
  connServer.addr = from
  connClient.addr = from
  loopbackQueue[#loopbackQueue + 1] = connClient
  computer.pushSignal("loopback_accept",from,port)
  return connServer
 end
 local nport = math.random(net.minport,net.maxport)
 local sclose = net.genPacketID()
 net.rsend(from,rport,tostring(nport))
 net.rsend(from,nport,sclose)
 return socket(from,nport,sclose)
end

function net.flisten(port,listener)
 local loopbackQueue = loopbackAccepted[port] or {}
 loopbackAccepted[port] = loopbackQueue
 local function helper(e,from,rport,data)
  if e == "net_msg" then
   if rport == port and data == "openstream" then
    local nport = math.random(net.minport,net.maxport)
    local sclose = net.genPacketID()
    net.rsend(from,rport,tostring(nport))
    net.rsend(from,nport,sclose)
    listener(socket(from,nport,sclose))
   end
  elseif e == "loopback_connect" and rport == port then
   local connServer, connClient = socketpair()
   connServer.addr = from
   connClient.addr = from
   loopbackQueue[#loopbackQueue + 1] = connClient
   computer.pushSignal("loopback_accept",from,port)
   listener(connServer)
  end
 end
 event.listen("net_msg",helper)
 event.listen("loopback_connect",helper)
 return helper
end

function net.ignore(helper)
 event.ignore("net_msg",helper)
 event.ignore("loopback_connect",helper)
end

return net
