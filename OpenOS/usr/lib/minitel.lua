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

local dgramQueues = setmetatable({},{__mode="v"})

local function dgramListener(_,from,port,data)
 local shouldNotify = false
 local portQueues = dgramQueues[port]
 if type(portQueues) ~= "table" then
  return
 end
 for queue,active in pairs(portQueues) do
  if type(queue) == "table" and active and (queue.addr == nil or queue.addr == from) then
   if #queue == 0 then
    shouldNotify = true
   end
   queue[#queue+1] = {addr=from,data=data}
   if type(queue.nextCb) == "function" then
    pcall(queue.nextCb,table.remove(queue,1))
    queue.nextCb = nil
   end
  end
 end
 -- wake waiting threads up
 if shouldNotify then
  event.push("dummy")
 end
end

local function listenDgrams(addr,port)
 local queue = {addr=addr,port=port}
 local portQueues = dgramQueues[port]
 if type(portQueues) ~= "table" then
  portQueues = setmetatable({},{__mode="k"})
  dgramQueues[port] = portQueues
 end
 queue.backref = portQueues
 portQueues[queue] = true
 return queue
end

local function unlistenDgrams(queue)
 local portQueues = dgramQueues[queue.port]
 portQueues[queue] = nil
 for otherQueue in pairs(portQueues) do
  if type(otherQueue) == "table" then
   return
  end
 end
 dgramQueues[queue.port] = nil
end

local function onNextDgram(queue, f)
 if #queue == 0 then
  queue.nextCb = f
 else
  pcall(f,table.remove(queue,1))
  event.push("dummy")
 end
end

event.listen("net_msg",dgramListener)

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

local function cwrite(self,data)
 if self.state == "open" then
  if not net.send(self.addr,self.port,data) then
   self:close()
   return false, "timed out"
  end
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

local function socket(addr,port,sclose)
 local conn = {}
 conn.addr,conn.port = addr,tonumber(port)
 conn.rbuffer = ""
 conn.write = cwrite
 conn.read = cread
 conn.state = "open"
 conn.sclose = sclose
 local first = true
 function conn.listener(_,f,p,d)
  if f == conn.addr and p == conn.port then
   if d == sclose then
    if not first then
     conn:close()
    end
   else
    conn.rbuffer = conn.rbuffer .. d
   end
   first = false
  end
 end
 event.listen("net_msg",conn.listener)
 function conn.close(self)
  event.ignore("net_msg",conn.listener)
  conn.state = "closed"
  net.rsend(addr,port,sclose)
 end
 return conn
end

function net.open(to,port)
 local queue = listenDgrams(to,port)
 if not net.rsend(to,port,"openstream") then
  unlistenDgrams(queue)
  return false, "no ack from host"
 end
 local st = computer.uptime()+net.streamdelay
 local est = false
 local data = nil
 local estQueue = nil
 local function portCb(msg)
  if msg.data ~= "openstream" then
   data = msg.data
   if tonumber(data) then
    est = true
    data = tonumber(data)
    estQueue = listenDgrams(to,data)
   end
  end
 end
 while not data and st >= computer.uptime() do
  onNextDgram(queue, portCb)
  if data then
   break
  end
  event.pull(net.streamdelay)
 end
 if not data then
  unlistenDgrams(queue)
  return nil, "timed out"
 end
 unlistenDgrams(queue)
 if not est then
  return nil, "refused"
 end
 local conn = nil
 local function scloseCb(msg)
  local sclose = msg.data
  conn = socket(to,data,sclose)
  while #estQueue > 0 and conn.state == "open" do
   conn.listener("net_msg",to,data,table.remove(estQueue,1).data)
  end
 end
 while not conn and st >= computer.uptime() do
  onNextDgram(estQueue, scloseCb)
  if conn then
   break
  end
  event.pull(net.streamdelay)
 end
 unlistenDgrams(estQueue)
 if not conn then
  return nil, "timed out"
 end
 return conn
end

function net.listen(port)
 local queue = listenDgrams(nil,port)
 local from = nil
 repeat
  event.pull()
  local msg = table.remove(queue,1)
  if msg and msg.data == "openstream" then
   from = msg.from
  end
 until from ~= nil
 local conn = socket(from,nport,sclose)  -- TODO stop listening on garabage-collected sockets
 local nport = math.random(net.minport,net.maxport)
 local sclose = net.genPacketID()
 net.rsend(from,rport,tostring(nport))
 net.rsend(from,nport,sclose)
 return conn
end

function net.flisten(port,listener)
 local function helper(_,from,rport,data)
  if rport == port and data == "openstream" then
   local nport = math.random(net.minport,net.maxport)
   local sclose = net.genPacketID()
   net.rsend(from,rport,tostring(nport))
   net.rsend(from,nport,sclose)
   listener(socket(from,nport,sclose))
  end
 end
 event.listen("net_msg",helper)
 return helper
end

return net
