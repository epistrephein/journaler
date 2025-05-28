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
        all_dfs = [query.getAllJournals() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithTitle(self, partialTitle):
        all_dfs = [query.getJournalsWithTitle(partialTitle) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsPublishedBy(self, partialName):
        all_dfs = [query.getJournalsPublishedBy(partialName) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithLicense(self, licenses):
        all_dfs = [query.getJournalsWithLicense(licenses) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithAPC(self):
        all_dfs = [query.getJournalsWithAPC() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithDOASeal(self):
        all_dfs = [query.getJournalsWithDOAJSeal() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

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

    def buildJournal(self, row):
        ids = [item for item in row["identifier"].split(",") if item]
        title = row["title"]
        languages = [item for item in row["languages"].split(", ") if item]
        publisher = row["publisher"]
        seal = row["seal"] == "Yes"
        licence = row["licence"]
        apc = row["apc"] == "Yes"

        return Journal(ids, title, languages, publisher, seal, licence, apc)

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
