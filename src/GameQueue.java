package ru.luvas.multiproxy.queues;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.AccessLevel;
import lombok.Getter;
import ru.luvas.multiproxy.MainClass;
import ru.luvas.multiproxy.entities.Party;
import ru.luvas.multiproxy.entities.Party.PartyMember;
import ru.luvas.multiproxy.packets.Packet115MultiThrow;
import ru.luvas.multiproxy.packets.Packet23GameShardJoin;
import ru.luvas.multiutils.Logger;
import ru.luvas.multiutils.sockets.RServer;

/**
 *
 * @author 0xC0deBabe <iam@kostya.sexy>
 */
public class GameQueue {
    
    @Getter
    private final String name;
    
    @Getter(AccessLevel.PACKAGE)
    private final Queue<QueuedServer> servers = new ConcurrentLinkedQueue<>();
    
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    @Getter(AccessLevel.PACKAGE)
    private final Queue<QueuedEntity> queued = new LinkedList<>();
    
    @Getter(AccessLevel.PACKAGE)
    private final Queue<QueuedEntity> prioritized = new LinkedList<>();
    
    @Getter(AccessLevel.PACKAGE)
    private final Queue<QueuedEntity> highestPriority = new LinkedList<>();
    
    GameQueue(String name) {
        QueueManager.queues.put(this.name = name, this);
    }
    
    void addServer(QueuedServer server) {
        this.servers.add(server);
    }
    
    void removeServer(QueuedServer server) {
        this.servers.remove(server);
    }
    
    void inqueue(String player, boolean prioritized) {
        lock.writeLock().lock();
        try {
            QueuedEntity qe = new QueuedEntity(player);
            if(prioritized)
                this.prioritized.add(qe);
            else
                this.queued.add(qe);
        }finally {
            lock.writeLock().unlock();
        }
    }
    
    void inqueue(Party party) {
        lock.writeLock().lock();
        try {
            queued.add(new QueuedEntity(party));
        }finally {
            lock.writeLock().unlock();
        }
    }
    
    void unqueue(String player) {
        lock.writeLock().lock();
        try {
            unqueueUnsafe(queued, player);
            unqueueUnsafe(prioritized, player);
            unqueueUnsafe(highestPriority, player);
        }finally {
            lock.writeLock().unlock();
        }
    }
    
    private void unqueueUnsafe(Collection<QueuedEntity> queue, String player) {
        queue.removeIf(entity -> entity.getPlayer() != null && entity.getPlayer().equals(player));
    }
    
    void unqueue(Party party) {
        lock.writeLock().lock();
        try {
            unqueueUnsafe(queued, party);
            unqueueUnsafe(prioritized, party);
            unqueueUnsafe(highestPriority, party);
        }finally {
            lock.writeLock().unlock();
        }
    }
    
    private void unqueueUnsafe(Collection<QueuedEntity> queue, Party party) {
        queue.removeIf(entity -> entity.getParty() == party);
    }
    
    private QueuedServer getTargetServer() {
        long current = System.currentTimeMillis();
        while(true) {
            QueuedServer top = servers.peek();
            if(top == null)
                return null;
            int needs = top.getNeeds().get();
            long last = top.getLastRequest().get();
            if(needs != 0 && current - last < 5000l)
                return top;
//            if(needs == 0) {
//                if(MainClass.isDebug())
//                    Logger.log("Target server %s for queue %s is not valid (doesn't require players anymore).", top.getName(), getName());
//            }else if(current - last < 5000l)
//                if(MainClass.isDebug())
//                    Logger.log("Target server %s for queue %s is not valid (due to inactivity).", top.getName(), getName());
            servers.poll();
        }
    }
    
    int tick() {
        lock.writeLock().lock();
        try {
            if(queued.isEmpty() && prioritized.isEmpty() && highestPriority.isEmpty()) {
//                if(MainClass.isDebug())
//                    Logger.log("There are no one in queue %s.", getName());
                return 0;
            }
            QueuedServer top = getTargetServer();
            if(top == null) {
//                if(MainClass.isDebug())
//                    Logger.log("Can not find target server for queue %s.", getName());
                return 0;
            }
//            if(MainClass.isDebug())
//                Logger.log("Target server for queue %s is %s (%d).", getName(), top.getName(), top.getNeeds().get());
            List<String> toThrow = new ArrayList<>();
            int passed = 0, left = top.getNeeds().get();
            while(left > 0 && !highestPriority.isEmpty()) {
                QueuedEntity entity = highestPriority.peek();
                if(entity.getParty() != null) {
                    int size = entity.getParty().getMembers().size();
                    if(size > left)
                        break;
                    highestPriority.poll();
                    left -= size;
                    entity.getParty().getMembers().stream().map(PartyMember::getName).forEach(toThrow::add);
                    QueueManager.removeQueuedUnsafe(entity.getParty());
                    passed += size;
                    continue;
                }
                highestPriority.poll();
                --left;
                toThrow.add(entity.getPlayer());
                QueueManager.removeQueuedUnsafe(entity.getPlayer());
                ++passed;
            }
            while(left > 0 && !prioritized.isEmpty()) {
                QueuedEntity entity = prioritized.poll();
                if(entity.getParty() != null) {
                    int size = entity.getParty().getMembers().size();
                    if(size > left) {
                        highestPriority.add(entity);
                        continue;
                    }
                    left -= size;
                    entity.getParty().getMembers().stream().map(PartyMember::getName).forEach(toThrow::add);
                    QueueManager.removeQueuedUnsafe(entity.getParty());
                    passed += size;
                    continue;
                }
                --left;
                toThrow.add(entity.getPlayer());
                QueueManager.removeQueuedUnsafe(entity.getPlayer());
                ++passed;
            }
            while(left > 0 && !queued.isEmpty()) {
                QueuedEntity entity = queued.poll();
                if(entity.getParty() != null) {
                    int size = entity.getParty().getMembers().size();
                    if(size > left) {
                        highestPriority.add(entity);
                        continue;
                    }
                    left -= size;
                    entity.getParty().getMembers().stream().map(PartyMember::getName).forEach(toThrow::add);
                    QueueManager.removeQueuedUnsafe(entity.getParty());
                    passed += size;
                    continue;
                }
                --left;
                toThrow.add(entity.getPlayer());
                QueueManager.removeQueuedUnsafe(entity.getPlayer());
                ++passed;
            }
            if(toThrow.isEmpty()) {
//                if(MainClass.isDebug())
//                    Logger.log("No one could be thrown for queue %s.", getName());
                return 0;
            }
//            if(MainClass.isDebug())
//                Logger.log("Throwing %s for queue %s.", toThrow.toString(), getName());
            String server;
            boolean withDelay = false;
            shardsHandler: {
                String[] spl = top.getName().split("\\$");
                server = spl[0];
                if(spl.length == 1)
                    break shardsHandler;
                int shardId = Integer.parseInt(spl[1]);
                RServer.getInstance().send(new Packet23GameShardJoin(shardId, toThrow), server);
                withDelay = true;
            }
            if(withDelay)
                MainClass.run(() -> RServer.getInstance().announce(new Packet115MultiThrow(server, toThrow)), 1, TimeUnit.SECONDS);
            else
                RServer.getInstance().announce(new Packet115MultiThrow(server, toThrow));
            top.setNeeds(left);
            return passed;
        }finally {
            lock.writeLock().unlock();
        }
    }
    
}
