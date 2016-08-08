package ru.luvas.multiproxy.queues;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import ru.luvas.multiproxy.MainClass;
import ru.luvas.multiproxy.entities.Party;
import ru.luvas.multiutils.Logger;

/**
 *
 * @author 0xC0deBabe <iam@kostya.sexy>
 */
public class QueueManager {

    final static ReadWriteLock qLock = new ReentrantReadWriteLock();
    final static Map<String, GameQueue> queues = new HashMap<>();
    
    final static ReadWriteLock pLock = new ReentrantReadWriteLock();
    final static Map<String, String> queuedPlayers = new HashMap<>();
    
    final static Map<String, QueuedServer> servers = new ConcurrentHashMap<>();
    
    public static void init() {
        MainClass.run(QueueManager::tick, 0l, 2l, TimeUnit.SECONDS);
        Logger.log("QueueManager initialized!");
    }
    
    private static void tick() {
        qLock.readLock().lock();
        pLock.writeLock().lock();
        try {
            queues.values().forEach(GameQueue::tick);
        }finally {
            qLock.readLock().unlock();
            pLock.writeLock().unlock();
        }
    }
    
    public static void addAsNewMemberOfParty(String player, Party party) {
        pLock.writeLock().lock();
        try {
            String partyQueue = queuedPlayers.get(party.getLeader().getName().toLowerCase());
            String playerQueue = queuedPlayers.remove(player.toLowerCase());
            qLock.readLock().lock();
            try {
                if(playerQueue != null)
                    queues.get(playerQueue).unqueue(player);
                if(partyQueue != null)
                    queuedPlayers.put(player.toLowerCase(), partyQueue);
            }finally {
                qLock.readLock().unlock();
            }
        }finally {
            pLock.writeLock().unlock();
        }
    }
    
    public static boolean inqueue(String queue, String player, boolean prioritized) {
        qLock.readLock().lock();
        try {
            GameQueue target = queues.get(queue);
            if(target == null)
                return false;
            pLock.writeLock().lock();
            try {
                String lplayer = player.toLowerCase();
                if(queuedPlayers.containsKey(lplayer))
                    return false;
                queuedPlayers.put(lplayer, queue);
            }finally {
                pLock.writeLock().unlock();
            }
            target.inqueue(player, prioritized);
        }finally {
            qLock.readLock().unlock();
        }
        return true;
    }
    
    public static boolean inqueue(String queue, Party party) {
        Set<String> players = party.getMembers().stream().map(m -> m.getName()).collect(Collectors.toSet());
        qLock.readLock().lock();
        try {
            GameQueue target = queues.get(queue);
            if(target == null)
                return false;
            pLock.writeLock().lock();
            try {
                Set<String> lowered = players.stream().map(String::toLowerCase).collect(Collectors.toSet());
                if(lowered.stream().anyMatch(queuedPlayers::containsKey))
                    return false;
                lowered.forEach(p -> queuedPlayers.put(p, queue));
            }finally {
                pLock.writeLock().unlock();
            }
            target.inqueue(party);
        }finally {
            qLock.readLock().unlock();
        }
        return true;
    }
    
    public static boolean unqueue(String player) {
        pLock.writeLock().lock();
        try {
            String queue = queuedPlayers.remove(player.toLowerCase());
            if(queue == null)
                return false;
            qLock.readLock().lock();
            try {
                queues.get(queue).unqueue(player);
                return true;
            }finally {
                qLock.readLock().unlock();
            }
        }finally {
            pLock.writeLock().unlock();
        }
    }
    
    public static boolean unqueue(Party party) {
        pLock.writeLock().lock();
        try {
            String queue = queuedPlayers.remove(party.getLeader().getName().toLowerCase());
            if(queue == null)
                return false;
            party.getMembers().stream().map(m -> m.getName().toLowerCase()).forEach(queuedPlayers::remove);
            qLock.readLock().lock();
            try {
                queues.get(queue).unqueue(party);
                return true;
            }finally {
                qLock.readLock().unlock();
            }
        }finally {
            pLock.writeLock().unlock();
        }
    }
    
    public static boolean isQueued(Collection<String> players) {
        pLock.readLock().lock();
        try {
            return players.stream().map(String::toLowerCase).allMatch(queuedPlayers::containsKey);
        }finally {
            pLock.readLock().unlock();
        }
    }
    
    public static boolean isQueued(String player) {
        pLock.readLock().lock();
        try {
            return queuedPlayers.containsKey(player.toLowerCase());
        }finally {
            pLock.readLock().unlock();
        }
    }
    
    public static boolean isQueued(String... players) {
        return isQueued(Arrays.asList(players));
    }
    
    public static boolean isQueued(Party party) {
        return isQueued(party.getMembers().stream().map(m -> m.getName()).collect(Collectors.toSet()));
    }
    
    public static void updateServer(String server, int needs) {
        QueuedServer qs = servers.get(server);
        if(qs == null) {
//            if(MainClass.isDebug())
//                Logger.log("Tried to update server %s (%d), but it is not registered.", server, needs);
            return;
        }
        qs.update(needs);
    }
    
    public static void registerServer(String server, String queue, int needs) {
//        if(MainClass.isDebug())
//            Logger.log("Registering server %s (%d) for queue %s.", server, needs, queue);
        QueuedServer qs = servers.get(server);
        if(qs == null) {
            qs = new QueuedServer(server, needs);
            servers.put(server, qs);
        }else
            qs.update(needs);
        GameQueue q;
        qLock.readLock().lock();
        try {
            q = queues.get(queue);
            if(q != null)
                qs.addQueue(q);
        }finally {
            qLock.readLock().unlock();
        }
        if(q == null) {
            qLock.writeLock().lock();
            try {
                q = new GameQueue(queue);
                qs.addQueue(q);
            }finally {
                qLock.writeLock().unlock();
            }
        }
    }
    
    static void removeServer(String server) {
        servers.remove(server);
    }
    
    static void removeQueuedUnsafe(Collection<String> players) {
        players.stream().map(String::toLowerCase).forEach(queuedPlayers::remove);
    }
    
    static void removeQueuedUnsafe(String player) {
        queuedPlayers.remove(player.toLowerCase());
    }
    
    static void removeQueuedUnsafe(String... players) {
        removeQueuedUnsafe(Arrays.asList(players));
    }
    
    static void removeQueuedUnsafe(Party party) {
        removeQueuedUnsafe(party.getMembers().stream().map(m -> m.getName()).collect(Collectors.toSet()));
    }
    
}
