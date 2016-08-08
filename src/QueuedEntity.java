package ru.luvas.multiproxy.queues;

import lombok.Getter;
import ru.luvas.multiproxy.entities.Party;

/**
 *
 * @author RinesThaix
 */
public class QueuedEntity {

    @Getter
    private final String player;
    
    @Getter
    private final Party party;
    
    public QueuedEntity(String player) {
        this.player = player;
        party = null;
    }
    
    public QueuedEntity(Party party) {
        this.party = party;
        this.player = null;
    }
    
}
