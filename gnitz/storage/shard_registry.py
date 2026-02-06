from gnitz.storage import errors

class ShardMetadata(object):
    """
    Metadata for a single shard.
    Tracks location, entity range, and LSN range.
    """
    def __init__(self, filename, component_id, min_eid, max_eid, min_lsn, max_lsn):
        self.filename = filename
        self.component_id = component_id
        self.min_entity_id = min_eid
        self.max_entity_id = max_eid
        self.min_lsn = min_lsn
        self.max_lsn = max_lsn


class ShardRegistry(object):
    """
    Registry tracking all active shards for efficient range queries
    and read amplification monitoring.
    """
    def __init__(self):
        # List of all registered shards
        self.shards = []
        # Compaction threshold (max read amplification before triggering compaction)
        self.compaction_threshold = 4
        # List of component IDs flagged for compaction
        self.components_needing_compaction = []
    
    def register_shard(self, metadata):
        """
        Registers a new shard in the registry.
        
        Args:
            metadata: ShardMetadata object
        """
        self.shards.append(metadata)
        self._sort_shards()
    
    def unregister_shard(self, filename):
        """
        Removes a shard from the registry by filename.
        
        Args:
            filename: Filename of the shard to remove
        
        Returns:
            True if shard was found and removed, False otherwise
        """
        for i in range(len(self.shards)):
            if self.shards[i].filename == filename:
                self.shards = self.shards[:i] + self.shards[i+1:]
                return True
        return False
    
    def _sort_shards(self):
        """Sorts shards by (component_id, min_entity_id) using insertion sort."""
        for i in range(1, len(self.shards)):
            key = self.shards[i]
            j = i - 1
            while j >= 0:
                current = self.shards[j]
                if current.component_id > key.component_id:
                    self.shards[j + 1] = self.shards[j]
                    j -= 1
                elif current.component_id == key.component_id and current.min_entity_id > key.min_entity_id:
                    self.shards[j + 1] = self.shards[j]
                    j -= 1
                else:
                    break
            self.shards[j + 1] = key
    
    def find_overlapping_shards(self, component_id, min_eid, max_eid):
        """
        Finds all shards that overlap with the given entity range.
        
        Args:
            component_id: Component type ID
            min_eid: Minimum entity ID of query range
            max_eid: Maximum entity ID of query range
        
        Returns:
            List of ShardMetadata objects that overlap the range
        """
        result = []
        for shard in self.shards:
            if shard.component_id != component_id:
                continue
            
            # Check if ranges overlap: [a1, a2] and [b1, b2] overlap if a1 <= b2 and b1 <= a2
            if shard.min_entity_id <= max_eid and min_eid <= shard.max_entity_id:
                result.append(shard)
        
        return result
    
    def get_read_amplification(self, component_id, entity_id):
        """
        Calculates the read amplification for a specific entity.
        
        Args:
            component_id: Component type ID
            entity_id: Entity ID to check
        
        Returns:
            Number of overlapping shards (read amplification factor)
        """
        count = 0
        for shard in self.shards:
            if shard.component_id == component_id:
                if shard.min_entity_id <= entity_id <= shard.max_entity_id:
                    count += 1
        return count
    
    def get_max_read_amplification(self, component_id):
        """
        Calculates the maximum read amplification for a component.
        
        Args:
            component_id: Component type ID
        
        Returns:
            Maximum read amplification factor
        """
        max_amp = 0
        
        # Check at min and max entity ID of each shard
        for shard in self.shards:
            if shard.component_id != component_id:
                continue
            
            amp = self.get_read_amplification(component_id, shard.min_entity_id)
            if amp > max_amp:
                max_amp = amp
            
            amp = self.get_read_amplification(component_id, shard.max_entity_id)
            if amp > max_amp:
                max_amp = amp
        
        return max_amp
    
    def mark_for_compaction(self, component_id):
        """
        Checks if a component needs compaction and marks it if so.
        
        Args:
            component_id: Component type ID to check
        
        Returns:
            True if component was marked for compaction, False otherwise
        """
        max_amp = self.get_max_read_amplification(component_id)
        
        if max_amp > self.compaction_threshold:
            # Check if already marked
            for comp_id in self.components_needing_compaction:
                if comp_id == component_id:
                    return True  # Already marked
            
            self.components_needing_compaction.append(component_id)
            return True
        
        return False
    
    def needs_compaction(self, component_id):
        """
        Checks if a component is marked as needing compaction.
        
        Args:
            component_id: Component type ID
        
        Returns:
            True if component needs compaction
        """
        for comp_id in self.components_needing_compaction:
            if comp_id == component_id:
                return True
        return False
    
    def clear_compaction_flag(self, component_id):
        """
        Clears the compaction flag for a component.
        
        Args:
            component_id: Component type ID
        """
        new_list = []
        for comp_id in self.components_needing_compaction:
            if comp_id != component_id:
                new_list.append(comp_id)
        self.components_needing_compaction = new_list
    
    def get_shards_for_component(self, component_id):
        """
        Returns all shards for a given component.
        
        Args:
            component_id: Component type ID
        
        Returns:
            List of ShardMetadata objects
        """
        result = []
        for shard in self.shards:
            if shard.component_id == component_id:
                result.append(shard)
        return result
