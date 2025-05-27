import json
import sqlite3
import pandas as pd
from rdflib import Graph, URIRef, Literal, RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get

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

        # Define class
        Journal = URIRef("https://schema.org/Periodical")

        # Define attributes of the class
        title = URIRef("https://schema.org/name")
        identifier = URIRef("https://schema.org/identifier")
        language = URIRef("https://schema.org/inLanguage")
        publisher = URIRef("https://schema.org/publisher")
        seal = URIRef("https://www.wikidata.org/wiki/Q73548471")
        license = URIRef("https://schema.org/license")
        apc = URIRef("https://www.wikidata.org/wiki/Q15291071")

        # Define base URL
        base_url = "https://github.com/epistrephein/journaler/"

        graph = Graph()

        df = pd.read_csv(path, keep_default_na=False)
        df["issn and eissn"] = df["Journal ISSN (print version)"] + "," + df["Journal EISSN (online version)"]

        for idx, row in df.iterrows():
            local_id = "journal-" + str(idx)
            subj = URIRef(base_url + local_id)

            graph.add((subj, RDF.type, Journal))
            graph.add((subj, title, Literal(row["Journal title"])))
            graph.add((subj, identifier, Literal (row["issn and eissn"])))
            graph.add((subj, language, Literal (row["Languages in which the journal accepts manuscripts"])))
            graph.add((subj, publisher, Literal(row["Publisher"])))
            graph.add((subj, seal, Literal(row["DOAJ Seal"])))
            graph.add((subj, license, Literal (row["Journal license"])))
            graph.add((subj, apc, Literal(row["APC"])))

        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl()

        store.open((endpoint, endpoint))
        data = graph.serialize(format="nt")
        query = f"INSERT DATA {{\n{data}\n}}"
        store.update(query)
        store.close()

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
        journals = []

        categories = set()
        areas = set()

        journals_categories = []
        journals_areas = []
        areas_categories = []

        # Normalize data
        for index, entry in enumerate(data):
            journal_id = index + 1
            identifiers = entry.get("identifiers", [])
            journals.append((
                journal_id,
                identifiers[0] if len(identifiers) > 0 else None,
                identifiers[1] if len(identifiers) > 1 else None,
            ))

            entry_categories = [
                (cat.get("id"), cat.get("quartile") or None)
                for cat in entry.get("categories", [])
            ]

            entry_areas = entry.get("areas", [])

            # Collect sets
            for cat in entry_categories:
                categories.add(cat)
                journals_categories.append((journal_id, cat))

            for area in entry_areas:
                areas.add(area)
                journals_areas.append((journal_id, area))

            # Area-category associations
            for area in entry_areas:
                for cat in entry_categories:
                    areas_categories.append((area, cat))

        # Deduplicate categories and areas
        category_id_map = {v: i+1 for i, v in enumerate(categories)}
        area_id_map = {v: i+1 for i, v in enumerate(areas)}

        # DataFrames
        df_journals = pd.DataFrame([
            {"id": jid, "identifier_1": id1, "identifier_2": id2}
            for jid, id1, id2 in journals
        ])

        df_categories = pd.DataFrame([
            {"id": cid, "name": name, "quartile": quartile}
            for (name, quartile), cid in category_id_map.items()
        ])

        df_areas = pd.DataFrame([
            {"id": aid, "name": name}
            for name, aid in area_id_map.items()
        ])

        df_journals_categories = pd.DataFrame([
            {"journal_id": jid, "category_id": category_id_map[c]}
            for jid, c in journals_categories
        ]).drop_duplicates()

        df_journals_areas = pd.DataFrame([
            {"journal_id": jid, "area_id": area_id_map[a]}
            for jid, a in journals_areas
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
            df_journals_categories.to_sql("journals_categories", con, index=False, if_exists="replace")
            df_journals_areas.to_sql("journals_areas", con, index=False, if_exists="replace")
            df_areas_categories.to_sql("areas_categories", con, index=False, if_exists="replace")

        return True

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        # Implemented in subclasses
        pass

class JournalQueryHandler(QueryHandler):
    BASE_QUERY = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX wiki: <https://www.wikidata.org/wiki/>

        SELECT ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
        WHERE {{
            ?journal rdf:type schema:Periodical ;
                     schema:name ?title ;
                     schema:identifier ?identifier ;
                     schema:inLanguage ?languages ;
                     schema:publisher ?publisher ;
                     wiki:Q73548471 ?seal ;
                     schema:license ?licence ;
                     wiki:Q15291071 ?apc .
                     {filter}
        }}
    """

    def __init__(self):
        super().__init__()

    def getAllJournals(self):
        filter = ""
        query = self.BASE_QUERY.format(filter=filter)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

    def getJournalsWithTitle(self, partialTitle):
        filter = f'FILTER(CONTAINS(LCASE(STR(?title)), LCASE("{partialTitle}")))'
        query = self.BASE_QUERY.format(filter=filter)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

    def getJournalsPublishedBy(self, partialName):
        filter = f'FILTER(CONTAINS(LCASE(STR(?publisher)), LCASE("{partialName}")))'
        query = self.BASE_QUERY.format(filter=filter)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

    def getJournalsWithLicense(self, licenses):
        if not licenses:
            return self.getAllJournals()
        else:
            conditions = []
            for lic in licenses:
                condition = f'CONTAINS(CONCAT(", ", STR(?licence), ", "), ", {lic}, ")'
                conditions.append(condition)

        filter = "FILTER(" + " || ".join(conditions) + ")"
        query = self.BASE_QUERY.format(filter=filter)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

    def getJournalsWithAPC(self):
        filter = 'FILTER(STR(?apc) = "Yes")'
        query = self.BASE_QUERY.format(filter=filter)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

    def getJournalsWithDOAJSeal(self):
        filter = 'FILTER(STR(?seal) = "Yes")'
        query = self.BASE_QUERY.format(filter=filter)

        endpoint = self.getDbPathOrUrl()
        df = get(endpoint, query, True)

        return df

class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            # Check if the area exists
            area_query = f"SELECT * FROM areas WHERE name = '{id}' LIMIT 1"
            area_df = pd.read_sql(area_query, con)
            if not area_df.empty:
                return area_df.assign(model="area")

            # Check if the category exists
            category_query = f"SELECT * FROM categories WHERE name = '{id}' LIMIT 1"
            category_df = pd.read_sql(category_query, con)
            if not category_df.empty:
                return category_df.assign(model="category")

            # Check if the journal exists
            journal_query = f"SELECT * FROM journals WHERE identifier_1 = '{id}' OR identifier_2 = '{id}' LIMIT 1"
            journal_df = pd.read_sql(journal_query, con)
            if not journal_df.empty:
                return journal_df.assign(model="journal")

        return pd.DataFrame()

    def getAllCategories(self):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT * FROM categories"
            df = pd.read_sql(query, con)

        return df.drop(columns=["id"])

    def getAllAreas(self):
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            query = "SELECT * FROM areas"
            df = pd.read_sql(query, con)

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
