package ru.luvas.multiproxy.queues;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AccessLevel;
import lombok.Getter;

/**
 *
 * @author 0xC0deBabe <iam@kostya.sexy>
 */
public class QueuedServer {

    @Getter
    private final String name;
    
    @Getter
    private AtomicInteger needs = new AtomicInteger();
    
    @Getter(AccessLevel.PACKAGE)
    private AtomicLong lastRequest = new AtomicLong();
    
    @Getter(AccessLevel.PACKAGE)
    private final Set<GameQueue> relatedQueues = new HashSet<>();
    
    public QueuedServer(String name, int needs) {
        this.name = name;
        this.needs.set(needs);
        this.lastRequest.set(System.currentTimeMillis());
    }
    
    public void update(int needs) {
        this.needs.set(needs);
        this.lastRequest.set(System.currentTimeMillis());
        setNeeds(needs);
    }
    
    void setNeeds(int value) {
        needs.set(value);
        if(value == 0)
            unregister();
    }
    
    public void addQueue(GameQueue queue) {
        relatedQueues.add(queue);
        queue.addServer(this);
    }
    
    public QueuedServer unregister() {
        relatedQueues.forEach(q -> q.removeServer(this));
        relatedQueues.clear();
        QueueManager.removeServer(name);
        return this;
    }
    
}
