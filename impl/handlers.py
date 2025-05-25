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

        # Initialize empty dictionary that we will populate before using as an input in the dataframe step (final)
        journals = {"id": [], "identifier_1": [], "identifier_2": []}
        categories = {"id": [], "name": [], "quartile": []}
        areas = {"id": [], "name":[]}
        journal_categories = {"journal_id": [], "category_id": []}
        journal_areas = {"journal_id" : [], "areas_id":[]}
        areas_categories = {"areas_id": [], "category_id": []}

        #normalize data for journal dataframe
        for index, entry in enumerate(data):
            journal_id = index + 1 
            identifiers = entry.get("identifiers", []) 
        #populate the dictionary previously set as empty 
            journals["id"].append(journal_id)
            journals["identifier_1"].append(identifiers[0] if len(identifiers) > 0 else None)
            journals["identifier_2"].append(identifiers[1] if len(identifiers) > 1 else None)

        #normalize categories 
        double_cat_quart_check = set()                   #categories are not univoque for each journal so we don't need them more than once
        cat_counter = 1
        for index, entry in enumerate(data):
            get_categories = entry.get("categories", []) #obtain the categories (dictionary in itself)
            for cat in get_categories:                   #for each category in the get_category we must associate 
                cat_name = cat.get("id")                 #the name to the cat_name variable
                quartile = cat.get("quartile")           #the quartile to the quartile variable 

                if (cat_name, quartile) not in double_cat_quart_check:
                    double_cat_quart_check.add((cat_name, quartile))

        #populate the empty dictionary for dataframe
                    categories["id"].append(cat_counter)
                    categories["name"].append(cat_name)
                    categories["quartile"].append(quartile)
                    cat_counter +=1 
        
        #normalize areas 
        double_area_check = set()
        area_counter = 1
        for index, entry in enumerate(data):
            get_areas = entry.get("areas", [])
            for area in get_areas:
                if area not in double_area_check:
                    double_area_check.add(area)
                    areas["id"].append(area_counter)
                    areas["name"].append(area)
                    area_counter += 1

        #normalize for journal+categories
        #create a dictionary on the fly that map category name to their corrisponding id 
        category_name_to_id = {name: id for id, name in zip(categories["id"], categories["name"])}
        for index, entry in enumerate(data):
            journal_id = index + 1
            get_categories = entry.get("categories", [])
            for cat in get_categories:
                cat_name = cat.get("id")
                cat_id = category_name_to_id.get(cat_name)

                if cat_id:  
                    journal_categories["journal_id"].append(journal_id)
                    journal_categories["category_id"].append(cat_id)
        
        #normalize journal+area
        #create a dictionary on the fly to map areas to their corrisponding id
        area_name_to_id ={name: id for id, name in zip(areas["id"], areas["name"])} 
        for index, entry in enumerate(data):
            journal_id = index + 1
            get_areas = entry.get("areas", [])
            for area in get_areas:
                area_id = area_name_to_id.get(area)
                if area_id:  
                    journal_areas["journal_id"].append(journal_id)
                    journal_areas["areas_id"].append(area_id)

        #normalize categories+ area
        # Normalize areas + categories relationship
        for index, entry in enumerate(data):
            get_areas = entry.get("areas", [])
            get_categories = entry.get("categories", [])
            for area in get_areas:
                area_id = area_name_to_id.get(area)
                for cat in get_categories:
                    cat_name = cat.get("id")
                    cat_id = category_name_to_id.get(cat_name)
                    if area_id and cat_id:
                        areas_categories["areas_id"].append(area_id)
                        areas_categories["category_id"].append(cat_id)


        # DataFrames step (takes input already normalized data)
        df_journals = pd.DataFrame(journals)

        df_categories = pd.DataFrame(categories)

        df_areas = pd.DataFrame(areas)

        df_journal_categories = pd.DataFrame(journal_categories).drop_duplicates()

        df_journal_areas = pd.DataFrame(journal_areas).drop_duplicates()

        df_area_categories = pd.DataFrame(areas_categories).drop_duplicates()

        # Save to SQLite
        with sqlite3.connect(self.getDbPathOrUrl()) as con:
            df_journals.to_sql("journals", con, index=False, if_exists="replace")
            df_categories.to_sql("categories", con, index=False, if_exists="replace")
            df_areas.to_sql("areas", con, index=False, if_exists="replace")
            df_journal_categories.to_sql("journal_categories", con, index=False, if_exists="replace")
            df_journal_areas.to_sql("journal_areas", con, index=False, if_exists="replace")
            df_area_categories.to_sql("areas_categories", con, index=False, if_exists="replace")

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
