import os, sys, re

#-------------- NESTED PATH CORRECTION --------------------------------#
# For all script files, we add the parent directory to the system path
import numpy as np
import pandas as pd

cwd = re.sub(r"[\\]", "/", os.getcwd())
cwd_list = cwd.split("/")
path = re.sub(r"[\\]", "/", sys.argv[0])
path_list = path.split("/")

# either the entire filepath is entered as command i python
if cwd_list[0:3] == path_list[0:3]:
    full_path = path
# or a relative path is entered, in which case we append the path to the cwd_path
else:
    full_path = cwd + "/" + path
# remove the overlap
root_dir = re.search(r"(^.+gender_in_academia)", full_path).group(1)
sys.path.append(root_dir)
#----------------------------------------------------------------------#


# -------------------------------------------------------------------------------------
#  The purpose of this code is to effectively parse publication lists in docx format.
# -------------------------------------------------------------------------------------

#from Models.initialize import *
import Models.MsWordModel as WM
from Functions.StringComp import *
from Models.DXT_doc_row import *
import Functions.DXT_functions as DXTF
from Functions.j_functions import *

# -------------------------------------------------------------------------------------
#  Doc_data is the object containing all researcher and resume data
# -------------------------------------------------------------------------------------
class Doc_data:
    def __init__(self):
        self.docs = [] # in this list we will have one list for each document
        # load the directory with docx files

        try:
            self.docx_directory = root_dir + '/Data/docx_files'
            self.filenames = os.listdir(self.docx_directory)
        except:
            pass

        # EXCEL COLUMNS RAW
        self.doc_atts = ['app_id', 'row_id', 'text', 'style', 'blank', 'section', 'year_heading', 'author_match',
                         'author_occ', 'personnummer', 'year', 'impact_factor', 'split_flag', 'ending_flag',
                         'ending_deduced', 'merge_below', 'skip_row', 'auth_list_loc', 'auth_list_loc_weak', 'asterisk_flag', 'deduce_reason',
                         'is_appendage']

        self.pubs = []
        # EXCEL COLUMNS PUB
        self.pub_atts = ['app_id', 'pub_id', 'VR_doctype', 'publication', 'year', 'api_match',
                         'api_id', 'title', 'doctype', 'journal', 'issue', 'volume', 'pub_date',
                         'pub_year', 'authors', 'keywords', 'is_international_collab',
                         'times_cited', 'impact_factor', 'journal_expected_citations',
                         'open_access', 'api_raw']

        self.researchers = []
        # EXCEL COLUMNS RESEARCHERS
        self.researcher_atts = ['app_id', 'name', 'surname', 'personnummer', 'extracted_pubs', 'matched_pubs', 'warnings']


    # ------------------------------------------------------------------------------
    #  The add_doc method will take a researcher app_id and import the word document
    # -----------------------------------------------------------------------------
    def add_doc(self, researcher):

        doc_rows = []
        # let us load the cv with the WordModel
        cv_filepath = self.docx_directory + "/" + str(researcher.app_id) + ".docx"

        # make sure that the filepath exists
        if not os.path.exists(cv_filepath):
            researcher.warnings.append("Missing CV")
            self.researchers.append(researcher)
            return False

        doc = WM.Docx_df(cv_filepath)


        # first we push the header to output
        doc_rows.append(Doc_row(researcher, "h", doc.header_text, "header", self.doc_atts))
        # then we loop through the final_df
        for index, row in doc.final_df.iterrows():
            doc_rows.append(Doc_row(researcher, index, row['text'], row['style'], self.doc_atts))

        # push the footer to output
        #doc_rows.append(Doc_row(researcher, "f", doc.header_text, "footer", self.doc_atts))

        # append the doc rows to self.docs
        self.docs.append(doc_rows)

        # append the researcher
        self.researchers.append(researcher)

        # now run the analysis
        self.analyze_doc(doc_rows)


    # ------------------------------------------------------------------------------
    #  merge rows into pub and append all pubs to self.pubs
    # -----------------------------------------------------------------------------
    def analyze_doc(self, doc):

        # get the researcher data
        r = self.researchers[-1]

        # initialize lists to be filled
        personnummer_candidates = []
        pubs = []

        # initialize inter-loop variables
        section = None
        year_heading = None
        pub_id = 1


        # ------------------------------------------------------------------------------
        #  Macro level, looking at the document as a whole
        # -----------------------------------------------------------------------------

        # Determine if blank rows separate entries (this is usefule for pop science and other pubs
        blanks_separate = DXTF.blanks_separate(doc)

        # Quick loop and check for ending_flag followed by blank
        # IDK if this is necessary. But this might be required, to idetify different types of documents.





        # ------------------------------------------------------------------------------
        #  Loop through all of the doc rows
        # -----------------------------------------------------------------------------

        for i in range(0, len(doc)):

            # ------------------------------------------------------------------------------
            #  Specify rows references for iteration
            # -----------------------------------------------------------------------------

            # specify the current row
            row = doc[i]

            # specify previous row
            if i == 0:
                previous_row = Doc_row_blank()
            else:
                previous_row = doc[i-1]

            # specify previous non-blank row
            j = 0
            while True:
                if i - 1 - j < 0:
                    previous_row_no_blank = Doc_row_blank()
                    break
                else:
                    previous_row_no_blank = doc[i-1-j]
                    if previous_row_no_blank.blank == True:
                        j = j + 1
                        continue
                    else: break

            # specify the next row
            if i == (len(doc) - 1):
                next_row = Doc_row_blank()
            else:
                next_row = doc[i+1]



            # ------------------------------------------------------------------------------
            #  Conditions for skipping a row at beginning of a new pub
            # -----------------------------------------------------------------------------

            # If it has already been decided that the row should be skipped, we skip the row
            # This can be determined in the initial doc_row analysis, or in the looping below.
            if row.skip == True:
                continue

            # If we are starting a new pub with blank, we skip
            if row.blank:
                continue

            # if the author wrote out None, NA, or -, then we skip
            if row.text.lower() in ["none", "-", 'na']:
                continue

            # before we skip header, check for personnummer
            # append personnummer to candidates list
            if row.author_match == True and row.personnummer != "":
                personnummer_candidates.append(row.personnummer)

            # skip appending header or footer as a publication
            if row.row_id in ["h", "f"]:
                continue



            # ------------------------------------------------------------------------------
            #  Section setting or Year heading
            # -----------------------------------------------------------------------------

            if row.section != None:
                section = row.section
                year_heading = None

                # check if the blanks separate the section
                sub_increment = 0
                blanks_in_section = 0
                while True:
                    sub_increment = sub_increment + 1
                    if i + sub_increment == len(doc):
                        break
                    sub_row = doc[i + sub_increment]
                    if sub_row.blank == True:
                        blanks_in_section = blanks_in_section + 1
                    if sub_row.section != None:
                        break

                percent_blanks_in_section = blanks_in_section / sub_increment
                blanks_separate_section = False
                if blanks_in_section >= 1 and percent_blanks_in_section > 0.2:
                    blanks_separate_section = True

            # determine year_heading by blank surround
            if previous_row.blank == True and next_row.blank == True:
                if row.text.strip() == str(row.year):
                    row.year_heading = row.year
                    row.year = None


            # set a year_heading
            if row.year_heading != None:
                year_heading = row.year_heading

            # Now that we have identified the section/year_heading, we can skip to next iteration
            if row.section != None or row.year_heading != None:
                continue

            # ------------------------------------------------------------------------------
            #  Сontrol for citation braggers
            # -----------------------------------------------------------------------------

            if previous_row_no_blank.section != None or pub_id == 1:

                citation_brag = DXTF.check_citation_bragger(row)

                # if strong trigger matched, we skip for sure
                if citation_brag == "strong":
                    row.skip_row = True
                    continue

                # if weak trigger, we skip the row if there is no ending flag and no author name
                if row.author_match == None and row.ending_flag == None and citation_brag == "weak":
                    row.skip_row = True
                    continue



            # ------------------------------------------------------------------------------
            #  Start constructing a pub
            # -----------------------------------------------------------------------------

            # set initial pub_text
            pub_text = row.text

            # set the has_flags
            if row.author_match == True:
                has_author = True
            else:
                has_author = False

            if row.year != None:
                has_year = True
                pub_year = row.year
            else:
                has_year = False
                pub_year = None

            if row.ending_flag == True:
                has_ending = True
            else:
                has_ending = False



            # ------------------------------------------------------------------------------
            #  Scan next rows until pub completion
            # -----------------------------------------------------------------------------

            skip_append = False
            increment = 1
            blank_has_been_skipped = False
            has_author_list_weak = False

            # There are three conditions for a complete pub. The loop will continue to iterate until the
            # three conditions are met. the variable skip_append determines if the publication gets pushed.
            while has_author == False or has_year == False or has_ending == False:

                # deduce ending if we forecast all the way to the end of the document
                if i + increment == len(doc):
                    # there is no next row
                    doc[i + increment - 1].ending_deduced = True
                    doc[i + increment - 1].deduce_reason = "End of doc"
                    if has_author == False and has_year == False and has_ending == False:
                        skip_append = True
                    break

                # ------------------------------------------------------------------------------
                #  Declare forecast previous, previous_no_blank, and next row
                # -----------------------------------------------------------------------------

                # if not, forecast the next row
                fcast_next_row = doc[i + increment]
                # previous row will at first be the current "row", but if multiple iterations, it will be
                # a row between the initial "row" and the fcast_next_row
                fcast_previous_row = doc[i + increment - 1]

                blank_buffer = 0
                while True:
                    fcast_previous_row_no_blank = doc[i + increment - 1 - blank_buffer]
                    if fcast_previous_row_no_blank.blank == True:
                        blank_buffer = blank_buffer + 1
                        if (i + increment - 1 - blank_buffer) < 0:
                            fcast_previous_row_no_blank = doc[0]
                            break
                    else:
                        break



                #############################################################################################
                # check for a weak author list
                if fcast_previous_row.auth_list_loc_weak != None:
                    has_author_list_weak = True


                # if the blank_percentage is high enough that blanks are condidered separators
                # then deduce ending if the next row is blanks and author name is met.
                if blanks_separate and (has_author or has_author_list_weak) and has_ending and fcast_next_row.blank == True and fcast_previous_row.auth_list_loc not in ['end', 'both']:
                    fcast_previous_row.ending_deduced = True
                    fcast_previous_row.deduce_reason = "Blank Separation"
                    break


                #############################################################################################

                # if the current forecast row (fcast_previous_row) is not of style: Heading 1, but the next row is
                # of style: Heading 1. Then deduce ending
                if fcast_previous_row.style != "Heading 1" and fcast_next_row.style == "Heading 1" and fcast_next_row.section == None:

                    # disregard if the heading is just the author name
                    name_split = re.split(r"[-\s]", r.name)
                    surname_split = re.split(r"[-\s]", r.surname)
                    auth_name_list = [x.lower() for x in name_split] + [x.lower() for x in surname_split]
                    # add the author first name intital and blank
                    auth_name_list = auth_name_list + [str(name_split[0]).lower()[0], ""]
                    row_content_clean = []
                    row_content_list =  re.split(r"[,\.\s]+", fcast_next_row.text.lower())
                    for c in row_content_list:
                        if c not in auth_name_list:
                            row_content_clean.append(c)
                    row_content_clean = " ".join(row_content_clean)
                    if len(row_content_clean) < 3 or (len(row_content_clean)/len(fcast_next_row.text)) < 0.15:
                        print("DETECTED", fcast_next_row.text, "as an ilegitimate heading")
                        pass
                    elif fcast_next_row.asterisk_flag == True:
                        pass
                    else:
                        fcast_previous_row.ending_deduced = True
                        fcast_previous_row.deduce_reason = "Heading coming up"
                        break

                #############################################################################################

                # if the fcast_next_row is a section header, then set previous row as ending_deduced, and break
                if fcast_next_row.section != None:
                    # however, if has_year == False and has_ending == False, we have probably only been
                    # merging the first few introductory lines (personnummer, and a few blanks) before the
                    # first section. If that is the case, we should not set ending_deduced to True.
                    if has_year == False and has_ending == False:
                        skip_append = True
                    else:
                        fcast_previous_row.ending_deduced = True
                        fcast_previous_row.deduce_reason = "Section coming up"
                    break

                #############################################################################################
                check_double_violations = True


                # before we check for double_violations we allow for some exceptions
                if section != None:
                    if "Popular science" in section:
                        if blanks_separate and blanks_separate_section:
                            if fcast_next_row.blank == None:
                                check_double_violations = False
                            else:
                                fcast_previous_row.ending_deduced = True
                                fcast_previous_row.deduce_reason = "Next row blank and separates"
                                break

                if check_double_violations == True:
                    # if the next row does not cause double of any values, the assign merge_below to current row
                    if has_author == True and fcast_next_row.author_match == True:
                        # double violation
                        # exception: if the auth_list end matches up with auth_list beginning (excluding blanks)
                        if (fcast_previous_row_no_blank.auth_list_loc in ['end', 'both'] and fcast_next_row.auth_list_loc in ['begining', 'both']):
                            pass
                        else:
                            fcast_previous_row.ending_deduced = True
                            fcast_previous_row.deduce_reason = "Author double"
                            break
                    if has_year == True and fcast_next_row.year != None:
                        # execption, if there happens to be a year in a short [Epub print blabla 2014] appendage,
                        # then we should not deduce ending, but apppend.
                        if fcast_next_row.is_appendage == True:
                            pass

                        # exception if no ending flag so far, and the next row has ending flag, and the same year, but
                        # no author, then we can go ahead and merge.  <TAG 44>
                        # we further restrict so that this only happens if no blank has been passed in the process.
                        # elif has_ending == False and fcast_next_row.ending_flag == None and pub_year == fcast_next_row.year and fcast_next_row.author_match == None and blank_has_been_skipped == False:
                        #     pass

                        else:
                            # double violation
                            fcast_previous_row.ending_deduced = True
                            fcast_previous_row.deduce_reason = "Year double"
                            break
                    if has_ending == True and fcast_next_row.ending_flag == True:
                        # Exception if the row has no author, no year, just an ending, we append it
                        if fcast_next_row.author_match == None and fcast_next_row.year == None:
                            pass
                        else:
                            # double violation
                            fcast_previous_row.ending_deduced = True
                            fcast_previous_row.deduce_reason = "ending_flag double"
                            break

                #############################################################################################

                # if we reach this  part of the code, it means that the next row is compatible to merge
                # with the current row, therefore indicate such on the current row
                fcast_previous_row.merge_below = True
                fcast_next_row.skip = True

                pub_text = pub_text + " " + fcast_next_row.text
                # remove addional space if created above
                pub_text = re.sub(r"\s+", " ", pub_text)

                ## UPDATE THE has_author, has_year, and has_ending variables
                if has_author == False and fcast_next_row.author_match == True:
                    has_author = True
                if has_year == False and fcast_next_row.year != None:
                    has_year = True
                    pub_year = fcast_next_row.year
                if has_ending == False and fcast_next_row.ending_flag == True:
                    has_ending = True

                increment = increment + 1
                if fcast_next_row.blank == True:
                    blank_has_been_skipped = True



            # now that all the has_ qualifiers are met, we check if any of the rows that follow should be merged

                #pubs.append(Pub_row(r, pub_id, section, row.text, row.year))

            # check if the next row is really just an ending (i.e. ending flag required
            # and year flag optional). With percentage letters < 50%.
            # This would suggest that there is no relevant title or author names in the text.
            # Example: 2016-01017


            # Now we are forecasting to the future (Beyond the complete row) as there might be
            # cases where we have to merge with subsequent rows, not just prior ones.

            increment_before_forecast = increment

            while True:

                # if we are at the last row, then there is nothing to do. break.
                if i + increment == len(doc):
                    merge_subsequent = True
                    break

                # select the forecast rows
                fcast_next_row = doc[i + increment]
                #print("NEXT ROW IS", fcast_next_row.text[0:60], "inc is", increment, "[", fcast_next_row.ending_flag, fcast_next_row.author_match, fcast_next_row.year, fcast_next_row.section, (i + increment), len(doc))
                blank_buffer = 0
                while True:
                    fcast_previous_row = doc[i + increment - 1 - blank_buffer]
                    if fcast_previous_row.blank == True:
                        blank_buffer = blank_buffer + 1
                    else:
                        break

                # if the row is blank we break with merge
                if fcast_next_row.blank == True or fcast_next_row.skip == True:
                    merge_subsequent = True
                    break


                just_letters = re.sub(r"[0-9:\.,-;\(\)\\\/\s]", "", fcast_next_row.text)
                percent_letters = len(just_letters) / len(fcast_next_row.text)

                ### ALL OF THE FOLLOWING CONTINUE STATEMENTS ARE VALID FUTURE MERGES
                if fcast_next_row.asterisk_flag == True or fcast_next_row.is_appendage == True:
                    pass

                elif fcast_next_row.ending_flag == True and fcast_next_row.author_match == None and percent_letters < 0.5 and fcast_next_row.section == None:
                    #print("SHNIZZLE", fcast_next_row.text)
                    #print("inc is", increment, "inc_b4_4cast", increment_before_forecast)
                    #print("previous row is", fcast_previous_row.text)
                    pass

                # if the next row is blank merge forward
                elif fcast_next_row.ending_flag == None and fcast_next_row.author_match == None and fcast_next_row.year == None and fcast_next_row.section == None:
                    #print("SHNIZZLE", fcast_next_row.text)
                    #print("inc is", increment, "inc_b4_4cast", increment_before_forecast)
                    #print("previous row is", fcast_previous_row.text)
                    pass

                #elif (fcast_next_row.auth_list_loc == 'begining' and fcast_previous_row.auth_list_loc == "end")

                ### IF NOT MET, WE BREAK AND RESET TO BEFORE THE FUTURE FORECAST
                else:
                    # so if we scan forward up until an entry, rather than up until a blank, there are
                    # very few conditions that would allow snagging text from future publications
                    if fcast_previous_row.ending_flag == True and fcast_previous_row.author_match == None and fcast_previous_row.year == None:
                        merge_subsequent = True
                        print("SHIT HITTING THE FAN:", fcast_previous_row.text)



                    else:
                        merge_subsequent = False
                        break

                increment = increment + 1


            # DONE WITH THE FORWARD FUTURE LOOPING

            if merge_subsequent == True:
                for temp_increment in range(increment_before_forecast, increment):
                    fcast_next_row = doc[i + temp_increment]
                    fcast_previous_row = doc[i + temp_increment - 1]

                    # undo the ending deduction
                    fcast_previous_row.ending_deduced = None
                    fcast_previous_row.deduce_reason = None
                    fcast_next_row.ending_deduced = True
                    fcast_next_row.deduce_reason = "Merge subsequent"

                    fcast_previous_row.merge_below = True
                    fcast_next_row.skip = True
                    pub_text = pub_text + " " + fcast_next_row.text
                    # remove addional space if created above
                    pub_text = re.sub(r"\s+", " ", pub_text)


            ### Additional controls ####

            ## 1.

            if section == None:
                name_split = re.split(r"[-\s]", r.name)
                surname_split = re.split(r"[-\s]", r.surname)
                auth_year_list = [x.lower() for x in name_split] + [x.lower() for x in surname_split] + [str(pub_year)]

                if has_year == False or has_author == False or has_ending == False:
                    without_auth_year = ""
                    text_split = re.split(r"[,\s-]", pub_text.lower())

                    for t in text_split:
                        if t in auth_year_list or t == "" or t == " ":
                            pass
                        else:
                            without_auth_year = without_auth_year + t + " "

                    without_auth_year_percent = 100 * len(without_auth_year) / len(pub_text)
                    if without_auth_year_percent < 50:
                        continue

            ## 2. if no author, no end_flag, and pub_id == 1, then we continue
            # This deals with the table in the beginning of
            if has_author == False and has_ending == False and pub_id == 1:
                continue

            ## 3. if no section, and pub_id == 1, then we continue
            # this deals with beginning statistics in 2016-01093
            if section == None and pub_id == 1:
                continue





            ### FINALLY THE END #####################################

            # append the entry
            if skip_append == False:
                if pub_year == None and year_heading != None:
                    pub_year = year_heading
                pubs.append(Pub_row(r, pub_id, section, pub_text, pub_year))

                pub_id = pub_id + 1



            # Perhaps we need to deal with splits, if so we do it here SPLIT SPLIT SPLIT
            #if row.author_occ > 1:



        # Wrap up the analysis

        # conclude status personnummer
        personnummer_candidates = list(set(personnummer_candidates))
        if None in personnummer_candidates:
            personnummer_candidates.remove(None)
        if len(personnummer_candidates) > 1:
            r.warnings = "multiple personnummer"
            r.personnummer = str(personnummer_candidates)
        if len(personnummer_candidates) == 1:
            r.personnummer = personnummer_candidates[0]

        r.extracted_pubs = len(pubs)


        # append the list of pubs to self.pubs
        self.pubs.append(pubs)


    def match_publications(self, df_api_data, r):
        # take the last pubs list, as we want to match for each researcher as they are added.
        pubs = None
        for p in self.pubs:
            if p != []:
                if p[0].app_id == r.app_id:
                    pubs = p
                    break

        # lets get the researcher as well
        print("LENGTH OF THE RESEARHCERS LIST IS", len(self.researchers))
        print("THE RESEARCHER ID IS", r.ID)
        researcher = self.researchers[(int(r.r_ID) - 1)]
        researcher.ID = r.ID

        skip_round = False
        try:
            if pubs == None:
                skip_round = True
        except:
            pass

        try:
            if df_api_data == None:
                skip_round = True
        except:
            pass


        if skip_round == False:

            # for pub in pubs[0:2]:
            for pub in pubs:

                # now we loop through the api data and find matching titles
                match_data = []
                for index, row in df_api_data.iterrows():
                    match_to_append = None

                    # is there a way to skip all irrelevant matches

                    comp = StringComp(row.title, pub.publication, 1, 0)
                    print("CONDUCTING STRING COMP:", index, "out of", len(df_api_data))

                    # IF A COMPLETE SUBSET
                    if comp.subset == True:
                        print("pure match:", pub.publication)
                        match_data.append({'data': row, 'confident': True, 'percent_letters': 100})
                        # if we have a perfect match, we can break already
                        #deviation = abs(int(row.pub_year) - int(pub.year))
                        #if deviation == 0:
                        #    break

                    # CHECK FOR WEAK
                    elif comp.max_percent_letters_matched > 50 and comp.max_percent_words_matched > 70:
                        print("No match:", pub.publication)

                        if pub.year not in ["multiple", None]:
                            deviation = abs(int(row.pub_year) - int(pub.year))
                            print("   year deviation is:", abs(int(row.pub_year) - int(pub.year)))
                        elif pub.year == "multiple":
                            deviation = 0
                            print("  multiple years provided. Allowing for now")
                        else:
                            deviation = 100

                        if deviation == 0:
                            # make sure that all the authors match as well
                            # note: we might have to make et. al. adjustments in the future
                            author_score = 0
                            try:
                                api_authors = json.loads(row.authors)
                            except:
                                api_authors = []

                            for api_auth in api_authors:
                                api_surname_full = api_auth.split(", ")[0].lower()
                                api_surname_list = re.split(r"[\s-]+", api_surname_full)

                                for api_surname in api_surname_list:
                                    print("api_surname", api_surname)
                                    if only_letters(api_surname) in only_letters(pub.publication):
                                        author_score = author_score + 1
                                        break

                            print("AUTHOR SCORE IS:", author_score)
                            total_authors = len(api_authors)
                            print("TOTAL NUM AUTHORS:", total_authors)
                            # requiring all api authors to match with exception of et al in pdf publication
                            if total_authors <= 3 and total_authors == author_score:
                                match_to_append = {'data': row, 'confident': False, 'percent_letters': comp.max_percent_letters_matched}
                            elif total_authors == 4 and author_score >= 3:
                                match_to_append = {'data': row, 'confident': False, 'percent_letters': comp.max_percent_letters_matched}
                            elif (total_authors == 5 or total_authors == 6) and author_score >= 4:
                                match_to_append = {'data': row, 'confident': False, 'percent_letters': comp.max_percent_letters_matched}
                            elif (total_authors >= 7 and total_authors <= 10) and author_score >= 5:
                                match_to_append = {'data': row, 'confident': False, 'percent_letters': comp.max_percent_letters_matched}
                            elif total_authors >= 10 and (author_score / total_authors > 0.75):
                                match_to_append = {'data': row, 'confident': False, 'percent_letters': comp.max_percent_letters_matched}
                            elif re.search(r"et\.\s*al", pub.publication) != None:
                                match_to_append = {'data': row, 'confident': False, 'percent_letters': comp.max_percent_letters_matched}

                        if match_to_append != None:

                            # IF WE ALLOW FOR SPELLING MISTAKES, WE REQUIRE MAX 1 YEAR DEVIATION AND ALL AUTHORS TO MATCH
                            if comp.max_percent_letters_matched > 70 and comp.max_percent_words_matched > 70:
                                print("No match:", pub.publication)

                                if pub.year not in ["multiple", None]:
                                    deviation = abs(int(row.pub_year) - int(pub.year))
                                    print("   year deviation is:", abs(int(row.pub_year) - int(pub.year)))
                                elif pub.year == "multiple":
                                    deviation = 0
                                    print("  multiple years provided. Allowing for now")
                                else:
                                    deviation = 100

                                if deviation <= 1:
                                    # make sure that all the authors match as well
                                    # note: we might have to make et. al. adjustments in the future
                                    author_score = 0
                                    api_authors = json.loads(row.authors)
                                    for api_auth in api_authors:
                                        api_surname_full = api_auth.split(", ")[0].lower()
                                        api_surname_list = re.split(r"[\s-]+", api_surname_full)

                                        for api_surname in api_surname_list:
                                            print("api_surname", api_surname)
                                            if only_letters(api_surname) in only_letters(pub.publication):
                                                author_score = author_score + 1
                                                break

                                        if author_score > 8:
                                            break

                                    print("AUTHOR SCORE IS:", author_score)
                                    total_authors = len(api_authors)
                                    print("TOTAL NUM AUTHORS:", total_authors)
                                    # requiring all api authors to match with exception of et al in pdf publication
                                    if total_authors <= 3 and total_authors == author_score:
                                        match_to_append = {'data': row, 'confident': True, 'percent_letters': comp.max_percent_letters_matched}
                                    elif total_authors == 4 and author_score >= 3:
                                        match_to_append = {'data': row, 'confident': True, 'percent_letters': comp.max_percent_letters_matched}
                                    elif (total_authors == 5 or total_authors == 6) and author_score >= 4:
                                        match_to_append = {'data': row, 'confident': True, 'percent_letters': comp.max_percent_letters_matched}
                                    elif (total_authors >=  7 and total_authors <= 10) and author_score >= 5:
                                        match_to_append = {'data': row, 'confident': True, 'percent_letters': comp.max_percent_letters_matched}
                                    elif total_authors >= 10 and (author_score / total_authors > 0.75):
                                        match_to_append = {'data': row, 'confident': True, 'percent_letters': comp.max_percent_letters_matched}
                                    elif re.search(r"et\.\s*al", pub.publication) != None:
                                        match_to_append = {'data': row, 'confident': True, 'percent_letters': comp.max_percent_letters_matched}

                    if match_to_append != None:
                        match_data.append(match_to_append)
                        # if a very accurate match we can break as well
                        #break

                            ### HERE WE NEED TO BREAK

                # IF match data is empty, we loosen even more the restriction, but note this will only be indicated as SIMILAR
                if len(match_data) != 0:

                    print("length of the match data is", len(match_data))

                    # Now we will remove duplicates in the match_data
                    unique_match_data = []
                    match1_index = 0
                    for match1 in match_data:
                        # run against all previous matches, and skip appending if duplicate discovered
                        append_status = True
                        match2_index = 0
                        for match2 in match_data[:(match1_index+1)]:
                            if match1_index == match2_index:
                                continue
                            elif match1['data'].api_id == match2['data'].api_id:
                                # the api_id has already been appended, i.e. the match is a duplicate
                                # do not append
                                append_status = False

                                # If the already appended match is missing raw_result, but the current match has raw_result
                                # then remove the existing match, and set append to True
                                for unique_match in unique_match_data.copy():
                                    if unique_match['data'].api_id == match1['data'].api_id:
                                        if (unique_match['data'].raw_result == None or unique_match['data'].raw_result == "") and (match1['data'].raw_result != None or match1['data'].raw_result == ""):
                                            unique_match_data.remove(unique_match)
                                            print("conducting OVERWRITE")
                                            print("from:", unique_match['data'])
                                            print("to:", match1['data'])
                                            append_status = True

                            match2_index = match2_index + 1
                        if append_status == True:
                            unique_match_data.append(match1)
                        match1_index = match1_index + 1

                    # now if the length of the unique_match_data is 1, we add the match data to the pub
                    print("Processing:", pub.publication)
                    if len(unique_match_data) == 1:
                        if unique_match_data[0]['confident'] == True:
                            pub.api_match = "EXACT"
                        else:
                            pub.api_match = "SIMILAR"
                        print("EXACT MATCH:", unique_match_data)
                        api_record = unique_match_data[0]['data']
                        pub.api_id = api_record.api_id
                        pub.title = api_record.title
                        pub.doctype = api_record.doctype
                        pub.journal = api_record.journal
                        pub.issue = api_record.issue
                        pub.volume = api_record.volume
                        pub.pub_date = api_record.pub_date
                        pub.pub_year = api_record.pub_year
                        pub.authors = api_record.authors
                        pub.keywords = api_record.keywords
                        pub.is_international_collab = api_record.is_international_collab
                        # not spelling mistake in the api data below
                        pub.times_cited = api_record.times_cites
                        pub.impact_factor = api_record.impact_factor
                        pub.journal_expected_citations = api_record.journal_expected_citations
                        pub.open_access = api_record.open_access
                        pub.api_raw = api_record.raw_result

                        # authors	keywords	is_international_collab	times_cites	impact_factor	journal_expected_citations	open_access	jnci	percentile	raw_result
                    # if the lenght of the unique_match_data is > 1, then we need to investigate, push a flag.
                    elif len(unique_match_data) > 1:
                        # sort the unique matches on percent_letters
                        print(unique_match_data)
                        unique_match_data = sorted(unique_match_data, key = lambda i: i['percent_letters'],reverse=True)
                        pub.api_match = "MULTIPLE"

                        api_record = unique_match_data[0]['data']
                        pub.api_id = api_record.api_id
                        pub.title = api_record.title
                        pub.doctype = api_record.doctype
                        pub.journal = api_record.journal
                        pub.issue = api_record.issue
                        pub.volume = api_record.volume
                        pub.pub_date = api_record.pub_date
                        pub.pub_year = api_record.pub_year
                        pub.authors = api_record.authors
                        pub.keywords = api_record.keywords
                        pub.is_international_collab = api_record.is_international_collab
                        # not spelling mistake in the api data below
                        pub.times_cited = api_record.times_cites
                        pub.impact_factor = api_record.impact_factor
                        pub.journal_expected_citations = api_record.journal_expected_citations
                        pub.open_access = api_record.open_access

                        # if multiple make sure that the raw results is a list of the matches
                        pub.api_raw = json.dumps([m['data'].raw_result for m in unique_match_data])

                    else:
                        pub.api_match = "MISSING"


            total_missing = 0
            for pub in pubs:
                if pub.api_match == "MISSING":
                    total_missing = total_missing + 1

            researcher.matched_pubs = researcher.extracted_pubs - total_missing



    def export(self, filepath, save=True):
        raw_data = {}
        for a in self.doc_atts:
            raw_data[a] = []
        for d in self.docs:
            for row in d:
                for a in self.doc_atts:
                    raw_data[a].append(getattr(row, a))
        #print("RAW DATA IS", raw_data)
        raw_df = pd.DataFrame(raw_data)
        #output.to_excel(project_root + "/Spreadsheets/new_docx_analysis/" + filename, index=False)


        # EXPORT THE PUBLICATIONS
        # pub column name adjustments
        col_name_adjust = {'publication': 'VR_publication', 'year': 'VR_year'}

        pub_data = {}
        for a in self.pub_atts:
            if a in col_name_adjust.keys():
                pub_data[col_name_adjust[a]] = []
            else:
                pub_data[a] = []
        for p in self.pubs:
            for row in p:
                for a in self.pub_atts:
                    try:
                        if a in col_name_adjust.keys():
                            pub_data[col_name_adjust[a]].append(getattr(row, a))
                        else:
                            pub_data[a].append(getattr(row, a))
                    except:
                        if a in col_name_adjust.keys():
                            pub_data[col_name_adjust[a]].append(None)
                        else:
                            pub_data[a].append(None)

        pub_df = pd.DataFrame(pub_data)


        #### EXPORT THE RESEARCHER SHEET
        researcher_data = {}
        for a in self.researcher_atts:
            researcher_data[a] = []
        for r in self.researchers:
            for a in self.researcher_atts:
                try:
                    val_to_append = getattr(r, a)
                except:
                    val_to_append = None
                if val_to_append in [[]]:
                    val_to_append = None
                researcher_data[a].append(val_to_append)
        researcher_df = pd.DataFrame(researcher_data)
        researcher_df.rename(columns={'name': 'firstname'}, inplace=True)

        if save:
            # export to excel
            writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
            raw_df.to_excel(writer, 'Raw', index=False)
            pub_df.to_excel(writer, 'Publications', index=False)
            researcher_df.to_excel(writer, 'Researchers', index=False)
            writer.save()

        return pub_df

    def import_dfs(self, raw_df, pub_df, res_df):

        # IMPORT THE RAW DATA
        doc_rows = []
        prev_app_id = None
        bool_cols = ['blank', 'author_match', 'ending_flag', 'ending_deduced', 'merge_below']
        for index, row in raw_df.iterrows():
            for a in self.doc_atts:
                val = getattr(row, a)
                if type(val) == float:
                    if math.isnan(val):
                        val = None
                        setattr(row, a, val)
                if a in bool_cols:
                    if val == 1:
                        bool_val = True
                    elif val == 0:
                        bool_val = False
                    elif val == None:
                        bool_val = None
                    else:
                        raise ValueError("BOOLEAN CONVERSION PROBLEM")
                    setattr(row, a, bool_val)
                #print("Attribute", a, "is", val)
            if (row.app_id == prev_app_id or prev_app_id == None) and index < len(raw_df) - 1:
                doc_rows.append(row)
            else:
                #print("previous app_id", prev_app_id, "now app_id", row.app_id)
                self.docs.append(doc_rows)
                doc_rows = []
                doc_rows.append(row)
            prev_app_id = row.app_id

        # IMPORT THE PUBS
        cols_to_rename = {'VR_publication': 'publication', 'VR_year': 'year'}
        pub_df.rename(columns=cols_to_rename, inplace=True)

        pub_list = []
        prev_app_id = None
        bool_cols = []
        for index, pub in pub_df.iterrows():
            for a in self.pub_atts:
                val = getattr(pub, a)
                if type(val) == float:
                    if math.isnan(val):
                        val = None
                        setattr(pub, a, val)
                #print("Attribute", a, "is", val)

            if (pub.app_id == prev_app_id or prev_app_id == None) and index < len(pub_df) - 1:
                pub_list.append(pub)
            else:
                # print("previous app_id", prev_app_id, "now app_id", row.app_id)
                self.pubs.append(pub_list)
                pub_list = []
                pub_list.append(pub)
            prev_app_id = pub.app_id


        # IMPORT THE RESEARCHERS
        cols_to_rename = {'name': 'firstname'}

        for index, r in res_df.iterrows():
            for a in self.researcher_atts:
                if a in cols_to_rename.keys():
                    val = getattr(r, cols_to_rename[a])
                    setattr(r, a, val)
                else:
                    val = getattr(r, a)
                if type(val) == float:
                    if math.isnan(val):
                        val = None
                        setattr(r, a, val)
                #print("Attribute", a, "is", val)
            self.researchers.append(r)


class Researcher_row:
    def __init__(self, researcher):
        self.app_id = researcher.app_id
        self.name = researcher['name']   # name is a reserved attribute
        self.surname = researcher.surname
        self.personnummer = None
        self.extracted_pubs = None
        self.matched_pubs = None
        self.warnings = []


class Pub_row:
    def __init__(self, researcher, pub_id, VR_doctype, text, year):
        self.app_id = researcher.app_id
        self.pub_id = pub_id
        self.VR_doctype = VR_doctype
        self.publication = text
        self.year = year




## HERE COMES THE MAIN FUNCTION #############################################################

def extract(researcher, save=True):

    # Doc_data is an object with a list doc
    # each element in the list is a list with the rows from a document.
    doc_data = Doc_data()

    # create researcher object from researcher data from VR
    r = Researcher_row(researcher)

    # here r is the researcher row in the Scrape6 spreadsheet
    print(r.app_id, r.name, r.surname)

    # add the document
    try:
        doc_data.add_doc(r)
    except:
        r.warnings.append("DATA ERROR")
        doc_data.researchers.append(Researcher_row(r))

    fp = f"{root_dir}/Data/docx_extractions/{r.app_id}.xlsx"

    return doc_data.export(fp, save)


def main():

    # import spreadsheet object of researcher data
    org_sheet = IM.scrape6()
    # start_num = 271
    # end_num = 300

    start_num = 13291
    end_num = 13291

    researchers = org_sheet.rows[(start_num - 1):end_num]
    filename = f"/single/analysis_{start_num}_{end_num}.xlsx"
    x = extract(researchers, filename, start_num, True)
    print(x)

if __name__ == "__main__":
    main()



