class QueryInterpreter:
    def build_filters(self, query: str):
        q = query.lower()

        filters = {"@and": []}

        if "electronics" in q:
            filters["@and"].append({"@eq": {"category": "electronics"}})

        if "home kitchen" in q or "home_kitchen" in q or "kitchen" in q:
            filters["@and"].append({"@eq": {"category": "home_kitchen"}})

        if "delivery" in q or "late" in q or "arrived" in q:
            filters["@and"].append({"@eq": {"complaint_type": "delivery_issue"}})

        if "missing" in q or "accessory" in q or "charger" in q or "cable" in q:
            filters["@and"].append({"@eq": {"complaint_type": "missing_parts"}})

        if "wrong item" in q or "wrong product" in q:
            filters["@and"].append({"@eq": {"complaint_type": "wrong_item"}})

        if "broken" in q or "failed" in q or "stopped working" in q or "not working" in q:
            filters["@and"].append({"@eq": {"complaint_type": "damage_defect"}})

        if len(filters["@and"]) == 0:
            return None

        if len(filters["@and"]) == 1:
            return filters["@and"][0]

        return filters