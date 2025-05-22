# copiato e incollato da githuh/nic/impl/handlers, e poi aggiustato

from rdflib import URIRef, Graph, RDF, Literal
import pandas as pd
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
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
        