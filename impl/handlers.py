import json
import sqlite3
import pandas as pd

class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, dbPathOrUrl):
        if isinstance(dbPathOrUrl, str):
            self.dbPathOrUrl = dbPathOrUrl
            return True
        else:
            return False

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        # Implemented in subclasses
        pass

class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        if not self.getDbPathOrUrl():
            return False

        print(f"Pushing {path} to {self.getDbPathOrUrl()}")
        return True

class CategoryUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        if not self.getDbPathOrUrl():
            return False

        # Read JSON file
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Initialize variables
        journals = [] # [(1, 'xxx', 'xxx')]
        journals = {"id": [], "identifier_1": [], "identifier_2": []}

        categories = set()
        areas = set()

        journal_categories = {"journal_id": [], "category_id": []}
        journal_areas = []
        areas_categories = []

        for index, entry in enumerate(data):
            journal_id = index + 1
            identifiers = entry.get("identifiers", [])
            journals["id"].append(journal_id)
            journals["identifier_1"].append(identifiers[0] if len(identifiers) > 0 else None)
            journals["identifier_2"].append(identifiers[1] if len(identifiers) > 1 else None)

            entry_categories = []
            for cat in entry.get("categories", []):
                cat_tuple = (cat.get("id"), cat.get("quartile"))
                entry_categories.append(cat_tuple)

            for cat in entry_categories:
                categories.add(cat)
                journal_categories.append((journal_id, cat))

            entry_areas = []
            entry_areas.extend(entry.get("areas", []))

            for area in entry_areas:
                areas.add(area)
                journal_areas.append((journal_id, area))

            for area in entry_areas:
                for cat in entry_categories:
                    areas_categories.append((area, cat))

        category_id_map = {}
        for i, v in enumerate(categories):
            category_id_map[v] = i+1

        area_id_map = {}
        for i, v in enumerate(areas):
            area_id_map[v] = i+1

        # DataFrames
        df_journals = pd.DataFrame(journals)

        df_categories = pd.DataFrame([
            {"id": cid, "name": name, "quartile": quartile}
            for (name, quartile), cid in category_id_map.items()
        ])

        df_areas = pd.DataFrame([
            {"id": aid, "name": name}
            for name, aid in area_id_map.items()
        ])

        df_journal_categories = pd.DataFrame([
            {"journal_id": jid, "category_id": category_id_map[c]}
            for jid, c in journal_categories
        ]).drop_duplicates()

        df_journal_areas = pd.DataFrame([
            {"journal_id": jid, "area_id": area_id_map[a]}
            for jid, a in journal_areas
        ]).drop_duplicates()

        df_areas_categories = pd.DataFrame([
            {"area_id": area_id_map[a], "category_id": category_id_map[c]}
            for a, c in areas_categories
        ]).drop_duplicates()

        # Save to SQLite
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            df_journals.to_sql("journals", con, index=False, if_exists="replace")
            df_categories.to_sql("categories", con, index=False, if_exists="replace")
            df_areas.to_sql("areas", con, index=False, if_exists="replace")
            df_journal_categories.to_sql("journal_categories", con, index=False, if_exists="replace")
            df_journal_areas.to_sql("journal_areas", con, index=False, if_exists="replace")
            df_areas_categories.to_sql("areas_categories", con, index=False, if_exists="replace")

        return True

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            # Check if the area exists
            area_query = f"SELECT * FROM areas WHERE name = '{id}'"
            area_df = pd.read_sql(area_query, con)
            if not area_df.empty:
                return area_df.drop(columns=["id"])

            # Check if the category exists
            category_query = f"SELECT * FROM categories WHERE name = '{id}'"
            category_df = pd.read_sql(category_query, con)
            if not category_df.empty:
                return category_df.drop(columns=["id"])

            # Check if the journal exists
            journal_query = f"SELECT * FROM journals WHERE identifier_1 = '{id}' OR identifier_2 = '{id}'"
            journal_df = pd.read_sql(journal_query, con)
            if not journal_df.empty:
                return journal_df.drop(columns=["id"])

            # TODO: add queries for blazegraph

        return pd.DataFrame()

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllJournals(self):
        # TODO: Implement this method
        pass

    def getJournalsWithTitle(self, partialTitle):
        # TODO: Implement this method
        pass

    def getJournalsPublishedBy(self, partialName):
        # TODO: Implement this method
        pass

    def getJournalsWithLicense(self, licenses):
        # TODO: Implement this method
        pass

    def getJournalsWithAPC(self):
        # TODO: Implement this method
        pass

    def getJournalsWithDOAJSeal(self):
        # TODO: Implement this method
        pass

class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllCategories(self):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            df = pd.read_sql("SELECT * FROM categories", con)

        return df.drop(columns=["id"])

    def getAllAreas(self):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            df = pd.read_sql("SELECT * FROM areas", con)

        return df.drop(columns=["id"])

    def getCategoriesWithQuartile(self, quartiles=set()):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            if not quartiles:
                query = "SELECT * FROM categories"
            else:
                q = ','.join(f"'{item}'" for item in quartiles if item is not None)
                query = f"SELECT * FROM categories WHERE quartile IN ({q})"
                if None in quartiles:
                    query += " OR quartile IS NULL"

            df = pd.read_sql(query, con)

        return df.drop(columns=["id"])

    def getCategoriesAssignedToAreas(self, area_ids=set()):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            if not area_ids:
                query = """
                    SELECT DISTINCT categories.name, categories.quartile
                    FROM categories
                    JOIN areas_categories ON categories.id = areas_categories.category_id;
                """
            else:
                a = ','.join(f"'{item}'" for item in area_ids)
                query = f"""
                    SELECT DISTINCT categories.name, categories.quartile
                    FROM categories
                    JOIN areas_categories ON areas_categories.category_id = categories.id
                    JOIN areas ON areas.id = areas_categories.area_id
                    WHERE areas.name IN ({a})
                """

            df = pd.read_sql(query, con)

        return df

    def getAreasAssignedToCategories(self, category_ids=set()):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            if not category_ids:
                query = """
                    SELECT DISTINCT areas.name
                    FROM areas
                    JOIN areas_categories ON areas.id = areas_categories.area_id;
                """
            else:
                c = ','.join(f"'{item}'" for item in category_ids)
                query = f"""
                    SELECT DISTINCT areas.name
                    FROM areas
                    JOIN areas_categories ON areas_categories.area_id = areas.id
                    JOIN categories ON categories.id = areas_categories.category_id
                    WHERE categories.name IN ({c})
                """

            df = pd.read_sql(query, con)

        return df
