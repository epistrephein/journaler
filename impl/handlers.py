import json
import sqlite3
import pandas as pd
from rdflib import URIRef, Graph, RDF, Literal
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
            raise ValueError("Database path or URL is not set")

        if not path.endswith(".csv"):
            raise ValueError("Input file is not in csv format")

        # class
        Journal = URIRef ("https://schema.org/Periodical")

        # attributes of the class
        title = URIRef("https://schema.org/name")
        identifier = URIRef("https://schema.org/identifier")####
        language = URIRef ("https://schema.org/inLanguage")
        publisher = URIRef ("https://schema.org/publisher")
        seal = URIRef ("https://www.wikidata.org/wiki/Q73548471")
        license = URIRef ("https://schema.org/license")
        apc = URIRef ("https://www.wikidata.org/wiki/Q15291071")

        base_url= "https://github.com/epistrephein/journaler/"
        df = pd.read_csv (path, keep_default_na=False)
        graph = Graph()
        df = df.head(100) # togliere prima della consegna!
        df["issn_eissn"] = df["Journal ISSN (print version)"] + "," + df["Journal EISSN (online version)"]
        df.head(20)

        for idx, row in df.iterrows():

            local_id = "journal-" + str(idx)
            subj = URIRef(base_url + local_id)

            graph.add ((subj, RDF.type, Journal))
            graph.add ((subj, title, Literal(row["Journal title"])))
            graph.add ((subj, identifier, Literal (row["issn_eissn"]))) ####
            graph.add ((subj, language, Literal (row["Languages in which the journal accepts manuscripts"])))
            graph.add ((subj, publisher, Literal(row["Publisher"])))
            graph.add ((subj, seal, Literal(row["DOAJ Seal"])))
            graph.add ((subj, license, Literal (row["Journal license"])))
            graph.add ((subj, apc, Literal(row["APC"])))

        store = SPARQLUpdateStore()
        endpoint = self.getDbPathOrUrl()
        store.open((endpoint, endpoint))
        for triple in graph.triples ((None, None, None)):
            store.add(triple)
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
        categories_set = set()
        areas_set = set()
        journal_categories = []
        journal_areas = []
        areas_categories = set()

        # Normalize data
        for idx, entry in enumerate(data):
            journal_id = idx + 1
            ids = entry.get("identifiers", [])
            journals.append({
                "id": journal_id,
                "identifier_1": ids[0] if len(ids) > 0 else None,
                "identifier_2": ids[1] if len(ids) > 1 else None,
            })

            entry_categories = [
                (cat.get("id"), cat.get("quartile") or None)
                for cat in entry.get("categories", [])
            ]
            entry_areas = entry.get("areas", [])

            # Collect sets
            for cat in entry_categories:
                categories_set.add(cat)
                journal_categories.append((journal_id, cat))

            for area in entry_areas:
                areas_set.add(area)
                journal_areas.append((journal_id, area))

            # Area-category associations
            for area in entry_areas:
                for cat in entry_categories:
                    areas_categories.add((area, cat))

        # Deduplicate categories and areas
        category_id_map = {v: i+1 for i, v in enumerate(categories_set)}
        area_id_map = {v: i+1 for i, v in enumerate(areas_set)}

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
        with sqlite3.connect("relational.db") as con:
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

        endpoint = self.getDbPathOrUrl()

        query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        PREFIX wiki: <https://www.wikidata.org/wiki/>

        SELECT ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
        WHERE {
        ?journal rdf:type schema:Periodical ;
                schema:name ?title ;
                schema:identifier ?identifier ;
                schema:inLanguage ?languages ;
                schema:publisher ?publisher ;
                wiki:Q73548471 ?seal ;
                schema:license ?licence ;
                wiki:Q15291071 ?apc .
        }
        """

        df_sparql = get(endpoint, query, True)
        return df_sparql

    def getJournalsWithTitle(self, partialTitle):

        endpoint = self.getDbPathOrUrl()

        query = f"""
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
                FILTER(CONTAINS {partialTitle})
        }}
        """

        df_sparql = get(endpoint, query, True)
        return df_sparql

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
                    SELECT DISTINCT areas.name FROM areas
                    JOIN areas_categories ON areas_categories.area_id = areas.id
                    JOIN categories ON categories.id = areas_categories.category_id
                    WHERE categories.name IN ({c})
                """

            df = pd.read_sql(query, con)

        return df
