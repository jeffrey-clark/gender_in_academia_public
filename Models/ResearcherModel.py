import pandas as pd


class Application:

    def __init__(self, row):
        self.id, self.app_id, self.year, self.surname, self.name, self.gender  = None, None, None, None, None, None
        self.financier, self.decision, self.clustered_grant_type, self.grant_type = None, None, None, None
        self.specialization, self.thematic_specialization, self.support_type,  = None, None, None
        self.deciding_body, self.field, self.assessing_group, self.all_scb_codes = None, None, None, None
        self.amount_granted, self.years_granted, self.project_title_swe = None, None, None
        self.project_title_eng, self.keywords = None, None

        # custom fields
        self.personnummer = None
        self.extracted_pubs, self.matched_pubs = 0, 0
        self.warnings = []

        if type(row) == pd.Series:
            cols = [x.split(" ")[0] for x in row.index.values]
            vals = tuple(row)
            for i in range(0, len(cols)):
                c, v = cols[i], vals[i]
                self.__setattr__(c, v)