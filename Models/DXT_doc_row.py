import re

# this is just a blank Doc_row
class Doc_row_blank:
    def __init__(self):
        self.section = None
        self.blank = True


#
# This Model conducts the flagging of doc_rows for the docxtractor_old.py
#

class Doc_row:
    def __init__(self, researcher, row_id, text, style, atts):

        # first we set all atts to None
        for a in atts:
            setattr(self, a, None)

        # declare attributes
        self.app_id = researcher.app_id
        self.row_id = row_id
        self.text = text
        self.style = style
        self.blank = None
        self.skip = None
        if len(self.text) == 0:
            self.blank = True

        self.auth_list_loc = None
        self.auth_list_loc_weak = None
        self.asterisk_flag = None
        self.deduce_reason = None
        self.is_appendage = None

        # make sure that all attributes that we want included in the excel are included
        # in the list in Doc_data.atts

        self.check_author_name(researcher)
        self.check_personnummer()
        self.check_impact_factor()
        self.check_year()
        self.check_flags(researcher)
        # identify the section last so that we can omit publications with the words publication in it.
        self.identify_section(researcher)
        self.check_asterisk_criteria()
        self.add_manual_skips()




    def identify_section(self, researcher):
        # If the row contains an author and year already, then it will most likely not be a section.
        if self.ending_flag == True:
            return None

        # if the authors name is present ant the length of the text is greater than 100, then we can confidently skip
        # (unless if we have to check for split flags in future)
        if self.author_match == True and len(self.text) > 100:
            if self.row_id != 0:
                return None


        section_types = {
            'Peer-reviewed publication':
                [
                    # criterion 1
                    [r'peer\W{0,4}review', r'peer\W{0,4}reviewed', r'referee\W{0,4}review',
                     r'referee\W{0,4}reviewed', r'peer\W{0,4}refereed',
                     r'bedömda', r'fackgranskade'],
                    # criterion 2
                    [r'publications*', r'articles*', r'papers*',
                     r'artiklar', r'publikation']   # Taken out: r'journals*'
                ],
            'Publication':
                [
                    # criterion 1
                    [r'publications*', r'articles*', r'papers*',
                     r'artiklar', r'publikation', r'original work']  # Taken out: r'journals*'
                ],
            'Peer-reviewed conference contribution':
                [
                    # criterion 1
                    [r'peer\W{0,4}review', r'peer\W{0,4}reviewed', r'referee\W{0,4}review',
                     r'referee\W{0,4}reviewed', r'peer\W{0,4}refereed',
                     r'bedömda', r'fackgranskade'],
                    # criterion 2
                    [r'conference contribution', r'konferensbidrag', r'conference']
                ],
            'Conference contribution':
                [
                    # criterion 1
                    [r'conference contribution', r'konferensbidrag', r"conference"]
                ],
            'Monograph':
                [
                    # criterion 1
                    [r'monographs*', r'manuscripts*',
                     r'monografier', r'manuskript']
                ],
            'Patent':
                [
                    # criterion 1
                    [r'patents*', r'patenter']
                ],
            'Developed software':
                [
                    # criterion 1
                    [r"software", r"programvara"]
                ],
            'Popular science publication':
                [
                    # criterion 1
                    [r"popular[\s-]+scien", r"populärvetenskap"]#,

                    # criterion 2
                    #[r'publications*', r'articles*', r'papers*',
                    # r'artiklar', r'publikationer']  # Taken out: r'journals*'
                ],
            'Popular science presentation':
                [
                    # criterion 1
                    [r"popular[\s-]+science", r"populärvetenskap"],

                    # criterion 2
                    [r'presentations*', r'presentationer']  # Taken out: r'journals*'
                ],
            'Other publication':
                [
                    # criterion 1
                    [r"other", r"report"],

                    # criterion 2
                    [r'publications*', r'articles*', r'papers*',
                     r'artiklar', r'publikationer', r"report"]  # Taken out: r'journals*'
                ],
            'Book/Chapter':
                [
                    # criterion 1
                    [r"books*", r"bok"],

                    # criterion 2
                    [r'chapters*', r'kapitel', r'letter']

                ],
            'Review article':
                [
                    # criterion 1
                    [r"reviews", r"review articles*", r'research review']

                ],
            'Computer Program':
                [
                    #criterion 1
                    [r"open access", r"developed"],

                    #criterion 2
                    [r"computer programs*", r"databases*"]
                ]
        }


        identified_sections = []

        key_list = list(section_types.keys())
        for key in key_list:
            criteria = section_types[key]
            criteria_to_meet = len(criteria)
            #print("NOW CHECKING", key, "with", criteria_to_meet, "criteria to meet" )
            score = 0
            for criterion in criteria:
                for c in criterion:
                    #print("Checking criterion:", c)
                    match = re.search(c, self.text.lower())
                    #print(self.text.lower(), match)
                    if match != None:
                        score = score + 1
                        break
                if score == criteria_to_meet:
                    identified_sections.append(key)
                    break

        # for advanced skipping determination, we store matches to be removed to make stripped_string
        matches_to_strip = []

        if len(identified_sections) > 0 and self.style not in ["Heading 1", "Title"]:
            for key in identified_sections:
                criteria = section_types[key]
                for criterion in criteria:
                    for c in criterion:
                        match = re.search(r"(" + c + r")", self.text.lower())
                        if match != None:
                            matches_to_strip.append(match.group(1))

            stripped_string = self.text.lower()

            # remove the matches
            for m in matches_to_strip:
                stripped_string = re.sub(re.escape(m), "", stripped_string)

            # remove any years
            stripped_string = re.sub(r"19[6789]\d|20[01]\d", "", stripped_string)

            # remove parentheses and their content
            stripped_string = re.sub(r"\(.+\)", "", stripped_string)

            # remove the author name
            name_split = re.split(r"[-\s]", researcher.name)
            surname_split = re.split(r"[-\s]", researcher.surname)
            auth_year_list = [x.lower() for x in name_split] + [x.lower() for x in surname_split]
            for a in auth_year_list:
                stripped_string = re.sub(re.escape(a), "", stripped_string)

            # remove other words that appear in section headers
            other_words = [
                r"original", r'phd', r'published', r"elsewhere",
                r"that you have developed", 'web of science'
            ]

            for o in other_words:
                stripped_string = re.sub(o, "", stripped_string)

            #print("text is", self.text.lower(), len(self.text.lower()))
            #print("stripped_string is", stripped_string, len(stripped_string))
            percentage = len(stripped_string) / len(self.text.lower())
            #print("percentage:", percentage)
            if percentage > 0.50:
                return None

        # make sure that peer-reviewed overwrites non-peer-reviewed
        # also make sure that other publication overwrites pulication

        #identified_sections_clean = []

        if len(identified_sections) == 1:
            self.section = identified_sections[0]
        elif len(identified_sections) > 1:
            priorities = [
                ('Peer-reviewed publication', 'Publication'),
                ('Peer-reviewed conference contribution', 'Conference contribution'),
                ('Other publication', 'Publication'),
                ('Review article', 'Publication'),
                ('Peer-reviewed conference contribution', 'Peer-reviewed publication'),
                ('Popular science publication', 'Publication'),
                ('Popular science publication', 'Popular science presentation'),
                ('Publication', 'Monograph'),
                ('Review article', 'Peer-reviewed publication')

            ]

            print(priorities)
            for p in priorities:
                if p[0] in identified_sections and p[1] in identified_sections:
                    identified_sections.remove(p[1])



            if len(identified_sections) == 1:
                self.section = identified_sections[0]
            elif len(identified_sections) > 1:
                self.section = str(identified_sections)

        else:
            self.section = None

        #  'Peer-reviewed conference contribution':
        #      ['Peer-reviewed conference contributions', 'bedömda konferensbidrag', 'conference contributions',
        #       'Conference presentations'],
        #  'Presentation':
        #      ['Presentation', 'Presentations'],
        #  'Computer software':
        #      ['Egenutvecklade allmänt tillgängliga datorprogram',
        #       'Open access computer programs', 'Publicly available computer programs', 'computer programs'],
        #  'Popular science':
        #      ['Popular science article', 'popular science', 'Popular-scientific articles',
        #       'Popular-Science Articles', 'Populärvetenskapliga artiklar'],
        #  'Review articles, book chapters, books':
        #      ['Review articles, book chapters, books', 'Reviews, book chapters, books', 'Book chapters'],
        #  'Supervision':
        #      ['supervision']
        #  }

    def check_author_name(self, researcher):

        if self.blank == True:
            return False

        full_first_name_matches = re.findall(r"\b" + re.escape(researcher.name) + r"\b", self.text)
        full_surname_matches = re.findall(r"\b" + re.escape(researcher.surname) + r"\b", self.text)

        if len(full_surname_matches) > 0 or len(full_first_name_matches) > 0:
            self.author_match = True
            if len(full_surname_matches) == 0:
                self.author_occ = len(full_first_name_matches)
            else:
                self.author_occ = len(full_surname_matches)


        split_surname = re.split(r'[\s-]', researcher.surname, 100)
        for s in split_surname:
            split_surname_matches = re.findall(r"\b" + re.escape(s) + r"\b", self.text)
            if len(split_surname_matches) > 0:
                self.author_match = True
                self.author_occ = len(split_surname_matches)


        # Now check for the author list location
        # i.e. are the authors listed at the end or the beginning of the entry
        # This only controls for big author lists
        if "," in self.text:
            #print("THE TEXT IS JFKDLSFJKL", self.text)
            #print("list is", self.text.split(","))
            trimmed_list = self.text.split(",")
            trimmed_list_clean = []
            for t in trimmed_list:
                if t != "":
                    # lets assume and author has maximum 3 names
                    skip_trim_append = False
                    if " " in t:
                        space_split = t.strip().split(" ")
                        while "" in space_split:
                            space_split.remove("")
                        if len(space_split) > 3:
                            skip_trim_append = True

                    if skip_trim_append == False:
                        trimmed_list_clean.append(t[0] + re.sub("[^0-9]", "", t))
                    else:
                        trimmed_list_clean.append("99")

            trimmed_text = ",".join(trimmed_list_clean)

            if len(trimmed_list_clean) > 10:
                trimmed_text_begining = ",".join(trimmed_list_clean[:8])
                trimmed_text_end = ",".join(trimmed_list_clean[-8:])
            else:
                trimmed_text_begining = trimmed_text
                trimmed_text_end = trimmed_text


            pattern_single_author = r"[^0-9]+"

            num_authors = 4
            pattern_multiple_authors = (pattern_single_author + re.escape(",")) * (num_authors+1)

            num_authors_weak = 2
            pattern_multiple_authors_weak = (pattern_single_author + re.escape(",")) * (num_authors_weak+1)

            ## MATCHING 4 AUTHORS
            beg_pattern = r"^" + pattern_multiple_authors + r"*"
            end_pattern = pattern_multiple_authors + r"*$" # include star as comma on eventual break is optional

            if re.search(beg_pattern, trimmed_text_begining) != None:
                self.auth_list_loc = "beginning"

            if re.search(end_pattern, trimmed_text_end) != None:
                if self.auth_list_loc != None:
                    self.auth_list_loc = "both"
                else:
                    self.auth_list_loc = "end"

            ## MATCHING 2 AUTHORS (WEAK)
            beg_pattern = r"^" + pattern_multiple_authors_weak + r"*"
            end_pattern = pattern_multiple_authors_weak + r"*$" # include star as comma on eventual break is optional

            if re.search(beg_pattern, trimmed_text_begining) != None:
                self.auth_list_loc_weak = "beginning"

            if re.search(end_pattern, trimmed_text_end) != None:
                if self.auth_list_loc_weak != None:
                    self.auth_list_loc_weak = "both"
                else:
                    self.auth_list_loc_weak = "end"



    def check_personnummer(self):
        queries = [r'\b(19[\d]{2}[01]\d[0123]\d)\W*([\d]{4})\b',
                   r'\b([\d]{2}[01]\d[0123]\d)\W*([\d]{4})\b']
        for query in queries:
            try:
                date_of_birth = re.search(query, self.text).group(1)
                secret_four = re.search(query, self.text).group(2)

                if date_of_birth and secret_four:
                    if len(date_of_birth) == 6:
                        date_of_birth = "19" + date_of_birth
                    self.personnummer = date_of_birth + "-" + secret_four
            except:
                pass

    def check_impact_factor(self):
        possible_notation = ["impact factor", "if"]
        for pn in possible_notation:
            try:
                self.impact_factor = re.search(re.escape(pn) + r'\s*[:=\s]\s*([\d.,]+)', self.text.lower()).group(1)
            except:
                pass

    def check_year(self):
        try:
            year_query = r'\b(19[6789]\d|20[01]\d)\b'
            year_matches = re.findall(year_query, self.text)
            unique_year_matches = list(set(year_matches))
            if len(unique_year_matches) == 1:
                self.year = unique_year_matches[0]
                if self.text.strip() == str(self.year) and re.search(r"Heading", self.style) != None:
                    self.year_heading = self.year
                    self.year = None
                # if the previous row is blank, the next row is blank, and the current row is just a year, this
                # is also a sign of a year_heading
                # This cannot be determined in here, but is determined in docxtractor...
            elif len(unique_year_matches) > 1:
                self.year = "multiple"
        except:
            pass



    def check_flags(self, researcher):
        self.ending_flag = None
        self.split_flag = None


        # make everything lowercase for matching to work
        name = researcher.name.lower()
        surname = researcher.surname.lower()
        text = self.text.lower()

        # future challenge: Clerencia M, Calderón A, Martínez N, Vergara I, Aldaz P, Poblador B, Machón M, Egüés N, Abellán G, Prados A. Multimorbidity patterns in hospitalized older patients: associations among chronic diseases and geriatric syndromes. PLoS One 2015;10(7):e0132909. IF: 3.73
        # i.e. end flags with occasional letters: e0132909

        mini_flags = []


        # sometimes there are specific letter combos in end that intervene with ending flag, clear these
        end_letters = [
            r"pp", r"p\d", r"volume", r"issue", r"pages*", r"january", r"february", r"march", r"april", r"may", r"june",
            r"july", r"august", r"september", r"october", r"november", r"december"
        ]

        for e in end_letters:
            text = re.sub(e, "", text)

        ## look for all end_mini_flags
        end_punctuation = r"[\:\-\(\)\;\,\/\.\–]+"

        # get rid of all space between punctuation
        text = re.sub(r"(" + end_punctuation + r")" + r"\s+" + r"(" + end_punctuation + r")", r'\g<1>\g<2>', text)

        # get rid of all space between digits
        text = re.sub(r"(\d+)\s+(\d+)", r'\g<1>\g<2>', text)
        print("DIGG", researcher.app_id, self.row_id)
        print(text)

        pattern = r"\s*\d+\s*" + end_punctuation + r"\s*\d+\s*" + end_punctuation + r"\s*\d+\s*"
        p = re.compile(pattern)
        for m in p.finditer(text):
            # if the ending flag is just a date, we can skip it
            v = m.group().strip()
            if re.search(r"[12][90]\d{2}-\d{2}-\d{2}$", v) != None:
                pass
            else:
                mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "ending"})

        pattern = end_punctuation + r"\s*\d{7,}.{,25}$"
        m = re.search(pattern, text)
        if m != None:
            mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "ending"})

        pattern = r"\(*in press\)*.*$"
        m = re.search(pattern, text)
        if m != None:
            mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "ending"})

        words_in_parenthesis_end = [
            'revision', 'comments', 'submitted', 'in press', 'epub ahead of print'
        ]
        for w in words_in_parenthesis_end:
            pattern = r"[\(\[].*" + re.escape(w) + r".*[\)\]]$"
            m = re.search(pattern, text)
            if m != None:
                mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "ending"})

        # Handle appendages
        appendage_conditions = [
            r"^epub.{,15}$",
        ]
        for c in appendage_conditions:
            if re.search(c, text) != None:
                self.is_appendage = True
                break




        ## look for author mini_flags

        # first run on the full-surname
        p = re.compile(surname + r"\b")
        for m in p.finditer(text):
            mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "author"})

        split_surname = re.split(r'[\s-]', surname)
        for s in split_surname:
            if len(s) / len(surname) < 0.2:
                # target to skip surnames like de or la
                pass
            else:
                print("S is", s)
                p = re.compile(re.escape(s) + r"\b")
                for m in p.finditer(text):
                    mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "author"})

        # if there are no authors mini_flags, then attempt to identify author by first name
        # then run on the full-first name
        author_found = False
        for dic in mini_flags:
            if dic['flag'] == "author":
                author_found = True
                break
        if author_found == False:
            p = re.compile(name + r"\b")
            for m in p.finditer(text):
                mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "author"})

        ## look for all year_miniflags
        year_query = r'\b([12][90][678901]\d)\b'
        p = re.compile(year_query)
        for m in p.finditer(text):
            mini_flags.append({'start': m.start(), 'end': m.end(), 'flag': "year"})

        # remove all subset miniflags
        unique_mini_flags = []
        i = 0
        for dic1 in mini_flags.copy():
            to_append = True
            for dic2 in mini_flags.copy()[:i]:
                if dic1['start'] >= dic2['start'] and dic1['end'] <= dic2['end']:
                    # dic 2, a previously appended dictionary, is a superset of dic1
                    # therefore we should not append dic1
                    to_append = False
            if to_append == True:
                unique_mini_flags.append(dic1)
            i = i + 1

        mini_flags = sorted(unique_mini_flags.copy(), key=lambda i: (i['start']))

        ## MINI_FLAG EVALUATION

        # Set a split flag if we have
        # AUTHOR, YEAR, AUTHOR, YEAR
        # YEAR, AUTHOR, YEAR, AUTHOR
        # the split element is a placeholder for where the split will take place
        #   the split will take place right before the element after the split placeholder.
        split_sequences = [
            ['author', 'year', 'split', 'author', 'year'],
            ['year', 'author', 'split', 'year', 'author'],
        ]

        self.split_point = None

        for split_sequence in split_sequences:
            sequence_index = 0
            fulfilled_index = len(split_sequence) - 1

            check_for_split_point = False
            for dic in mini_flags:
                if dic['flag'] == split_sequence[sequence_index]:
                    if check_for_split_point == True:
                        self.split_point = dic['start'] - 1
                        check_for_split_point = False
                    sequence_index = sequence_index + 1
                    if split_sequence[sequence_index] == "split":
                        check_for_split_point = True
                        sequence_index = sequence_index + 1
                if sequence_index == fulfilled_index:
                    self.split_flag = True
                    break

            if self.split_flag == True:
                break

        # determine ending_flag
        if len(mini_flags) > 0:
            if mini_flags[-1]['flag'] == 'ending':
                self.ending_flag = True
        if len(mini_flags) > 1:
            if mini_flags[-2]['flag'] == 'ending' and mini_flags[-1]['flag'] == 'year':
                self.ending_flag = True
        if len(mini_flags) > 2:
            if mini_flags[-3]['flag'] == 'ending' and mini_flags[-2]['flag'] == 'year' and mini_flags[-1]['flag'] == 'year':
                self.ending_flag = True

        if self.split_flag == True:
            string1 = text[:self.split_point]
            string2 = text[self.split_point:]

            #print("string 1 is:", string1)
            #print("string 2 is:", string2)

        #print("string is:", text)
        #print("ending flag:", self.ending_flag)
        #print("split flag:", self.split_flag)
        #print("split point", self.split_point)

        #print(mini_flags)

        # finally we check for endings like "[accepted for publication]
        if self.ending_flag == None:
            patterns = [
                #r"\[accepted for publication\]\s*$",
                r"\[epub\s+ahead\s+of\s+print\]\s*$",
                r"accepted\s+for\s+publication",
                r"citations:\s*\d+\w{,3}$"
            ]
            for p in patterns:
                if re.search(p, self.text.lower()) != None:
                    self.ending_flag = True
                    break


    def check_asterisk_criteria(self):
        text = self.text.lower()
        asterisk_criteria = [
            r"^[\*1].{,20}equal.*contrib.*$", r"^[\*1].{,20}contrib.*equal.*$"
        ]

        for c in asterisk_criteria:
            if re.search(c, text) != None:
                self.asterisk_flag = True
                break

        if self.asterisk_flag == True:
            self.section = None


    def add_manual_skips(self):
        text = self.text.lower()
        skip_criteria = [
            r"highlighted as viewpoint in", r"^page\s*\d+\s*of\s*\d+$"
        ]
        for c in skip_criteria:
            if re.search(c, text) != None:
                self.skip = True
