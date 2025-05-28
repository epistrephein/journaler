from impl.models import Category, Area, Journal
from impl.handlers import CategoryQueryHandler, JournalQueryHandler

import pandas as pd

class BasicQueryEngine:
    def __init__(self):
        self.journalQuery = []
        self.categoryQuery = []

    def cleanJournalHandlers(self):
        self.journalQuery.clear()
        return True

    def cleanCategoryHandlers(self):
        self.categoryQuery.clear()
        return True

    def addJournalHandler(self, handler):
        self.journalQuery.append(handler)
        return True

    def addCategoryHandler(self, handler):
        self.categoryQuery.append(handler)
        return True

    def getEntityById(self, id):
        # TODO: Implement this class
        pass

    def getAllJournals(self):
        # TODO: Implement this class
        pass

    def getJournalsWithTitle(self, partialTitle):
        # TODO: Implement this class
        pass

    def getJournalsPublishedBy(self, partialName):
        # TODO: Implement this class
        pass

    def getJournalsWithLicense(self, licenses):
        # TODO: Implement this class
        pass

    def getJournalsWithAPC(self):
        # TODO: Implement this class
        pass

    def getJournalsWithDOASeal(self):
        # TODO: Implement this class
        pass

    def getAllCategories(self):
        all_dfs = [query.getAllCategories() for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getAllAreas(self):
        all_dfs = [query.getAllAreas() for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        areas = []
        for index, row in merged_df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas

    def getCategoriesWithQuartile(self, quartiles):
        all_dfs = [query.getCategoriesWithQuartile(quartiles) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getCategoriesAssignedToAreas(self, area_ids):
        all_dfs = [query.getCategoriesAssignedToAreas(area_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getAreasAssignedToCategories(self, category_ids):
        all_dfs = [query.getAreasAssignedToCategories(category_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        areas = []
        for index, row in merged_df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas
    
    def getJournalsCategories(self, journal_id):
        all_dfs = [query.getJournalsCategories(journal_id) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()  

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)
        
        return categories 

    def getJournalsAreas(self, journal_id):
        all_dfs = [query.getJournalsAreas(journal_id) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()  

        areas = []
        for index, row in merged_df.iterrows():
            area = Area(row["name"])
            areas.append(area)
        
        return areas
    
    def buildJournal(self, row):
        ids = [item for item in row["identifier"].split(",") if item]
        title = row["title"]
        languages = [item for item in row["languages"].split(", ") if item]
        publisher = row["publisher"]
        seal = row["seal"] == "Yes"
        licence = row["licence"]
        apc = row["apc"] == "Yes"

        journal_id = ids[0]
        categories = self.getJournalsCategories(journal_id)
        areas = self.getJournalsAreas(journal_id)

        return Journal(ids, title, languages, publisher, seal, licence, apc, categories, areas)
    
class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles):
        # TODO: Implement this class
        pass

    def getJournalsInAreasWithLicense(self, areas_ids, licenses):
        # TODO: Implement this class
        pass

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles):
        # TODO: Implement this class
        pass
