LIST_OF_CONSUMABLES = [
    2003, # Health Potion
    2010, # Total Biscuit of Everlasting Will
    2031, # Refillable Potion
    2055, # Control Ward
    2138, # Elixir of Iron
    2139, # Elixir of Sorcery
    2140, # Elixir of Wrath
    2150, # Elixir of Skill
    2151, # Elixir of Avarice
    2152, # Elixir of Force
    3400, # Your Cut
    3513, # Eye of the Herald
]


class Inventory():
    def __init__(self):
        self.dict = {}
        self.list_of_events = []
        # max_size = 6 unique items + 1 ward + 1 consumable
        # The consumable will be instantly consumed because the inventory is full.
        self.max_size = 8

    def increment(self, item_id: str):
        self.dict[item_id] = self.dict.get(item_id, 0) + 1

    def decrement(self, item_id: str):
        self.dict[item_id] = max(0, self.dict.get(item_id, 0) - 1)
        if self.dict[item_id] == 0:
            self.dict.pop(item_id)

    def process_event(
        self,
        event_type: str,
        timestamp: int,
        item_id: int = None,
    ):
        """
        Update the inventory of a participant based on the event.
        """
        # list_of_ward_items = (3363, 3364, 3340)
        # existing_ward_items = [item for item in inventory if item in list_of_ward_items]
        if event_type in ('ITEM_PURCHASED', 'ITEM_SOLD', 'ITEM_DESTROYED'):
            self.list_of_events.append({
                'type': event_type,
                'timestamp': timestamp,
                'itemId': item_id
            })

        match event_type:
            case 'ITEM_PURCHASED':                    
                self.increment(item_id)
            case 'ITEM_SOLD':
                self.decrement(item_id)
            case 'ITEM_DESTROYED':
                self.decrement(item_id)
            case 'ITEM_UNDO':
                last_event = self.list_of_events.pop()

                # Undo item purchase
                if last_event['type'] == 'ITEM_PURCHASED':
                    self.decrement(last_event['itemId'])
                    
                    # Find all simultaneous ITEM_DETROYED events
                    # and revert them.
                    for event in reversed(self.list_of_events):
                        if (
                            event['timestamp'] == last_event['timestamp']
                            and event['type'] == 'ITEM_DESTROYED'
                        ):
                            self.increment(event['itemId'])
                            self.list_of_events.pop()
                        else:
                            break
                # Undo item sell
                elif last_event['type'] == 'ITEM_SOLD':
                    self.increment(last_event['itemId'])
        return self.dict

    def get_items_and_counts(self):
        list_of_item_ids_sorted = sorted(self.dict)
        list_of_counts_sorted = [self.dict[item_id] for item_id in list_of_item_ids_sorted]

        # Lists are padded to the max inventory size
        pad_list = lambda l: l + [0] * (self.max_size - len(l))
        return pad_list(list_of_item_ids_sorted), pad_list(list_of_counts_sorted)
