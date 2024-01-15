import re


class StringList(object):
    def __init__(self, input_list=None):
        if input_list == None:
            self.list = []
        else:
            self.list = input_list

        self.word_count = None
        self.letter_count = None

        # run methods
        self.update_count()

    def update_count(self):
        if self.list == None:
            pass
        else:
            self.word_count = len(self.list)
            self.letter_count = len("".join(self.list))

    def append(self, word):
        self.list.append(word)
        self.update_count()

    def remove(self, word):
        self.list.remove(word)
        self.update_count()

    def count(self, word):
        # returns the count i.e. occurance, of a word in a StringList
        return self.list.count(word)

    def copy(self, StringList):
        self.list = StringList.list.copy()
        self.update_count()


class SequenceList(StringList):
    def __init__(self, word1_index, word2_index, input_list=None):
        StringList.__init__(self, input_list)
        self.word1_index = word1_index
        self.word2_index = word2_index
        self.s1_start_index = word1_index
        self.s2_start_index = word2_index
        self.spelling_mistakes = False
        self.sequence_mistakes = False

    def print_sequence(self):
        print(f"Sequence found of length: {self.word_count}  Indices: s1: [{self.s1_start_index}, {self.word1_index}]  "
              f"s2: [{self.s2_start_index}, {self.word2_index}],     Seq.Mist.: {self.sequence_mistakes}, Spell.Mist.: {self.spelling_mistakes}")
        print(self.list)

# the StringComp String Object
class SCString(object):
    def __init__(self, input, comp_string):
        self.string = input
        self.clean_string = None
        # StringList of all the words in the string
        self.words = StringList()
        # we need to complement list as well, to compute match percentages
        #self.comp_list = StringList()
        # list for matched words
        self.matched = StringList()
        self.matched_weak = StringList()
        self.remainder = StringList()
        self.remainder_weak = StringList()

        self.percent_words_matched = None
        self.percent_letters_matched = None
        self.percent_words_matched_weak = None
        self.percent_letters_matched_weak = None

        # run methods
        self.make_word_list()
        #print("word list is", self.words.list)


    def make_word_list(self):
        # This method sets self.words and self.comp_list
        # process the main string
        s = self.string.lower().strip()
        c = re.sub(r'[.:;,/\[\]]*', "", s)
        self.clean_string = re.sub(r'\s+', " ", c)
        l = self.clean_string.split(" ")
        while True:
            if "" in l:
                l.remove("")
            else:
                break
        # the clean string has been split into a list, that is now stored as a StringList
        self.words = StringList(l)

        # process the comparison string
        # s = comp_string.lower().strip()
        # c = re.sub(r'[.:;,/\[\]]*', "", s)
        # l = re.sub(r'\s+', " ", c).split(" ")
        # while True:
        #     if "" in l:
        #         l.remove("")
        #     else:
        #         break
        # self.comp_list = StringList(l)

    def calculate_match(self):
        index = 0
        for string_list in [self.words, self.matched]:
            #print(index, string_list.list)
            string_list.update_count()
            index += 1

        if self.words.word_count != 0:
            self.percent_words_matched = 100 * self.matched.word_count / self.words.word_count
            self.percent_letters_matched = 100 * self.matched.letter_count / self.words.letter_count
            #self.percent_words_matched_weak = 100 * self.matched_weak.word_count / self.comp_list.word_count
            #self.percent_letters_matched_weak = 100 * self.matched_weak.letter_count / self.comp_list.letter_count
        else:
            self.percent_words_matched = 0
            self.percent_letters_matched = 0

class StringComp(object):

    def __init__(self, s1, s2, allowed_spelling_mistakes, allowed_sequence_mistakes):
        self.s1 = SCString(s1, s2)
        self.s2 = SCString(s2, s1)
        self.allowed_spelling_mistakes = allowed_spelling_mistakes
        self.allowed_sequence_mistakes = allowed_sequence_mistakes
        self.matched_both = StringList()
        self.identical = None
        # self.sequences is a list containing StringLists of matched sequences
        self.sequences = []

        self.subset = None
        # subset_weak allows for specified spelling and sequence mistakes
        self.subset_weak = None
        self.max_percent_words_matched = None
        self.max_percent_letters_matched = None
        self.max_percent_words_matched_weak = None
        self.max_percent_letters_matched_weak = None

        # run methods
        self.check_matches()
        self.calculate_matches()
        # self.both_word = []

    def new_sequence(self):
        self.sequences.append(StringList())
        return self.sequences[-1]


    def check_matches(self):
        # first we compute the exact match result
        if self.s1.clean_string == self.s2.clean_string:
            self.identical = True
        else:
            self.identical = False


        ## Now address sequence matches between the strings
        sequence_gaps = 0
        # new address sequence in separate loops to keep things understandable
        word1_index = 0
        word2_index = 0
        #print("s1.words.list:", self.s1.words.list)
        #print("s2.words.list:", self.s2.words.list)


        for word1 in self.s1.words.list:
            for word2 in self.s2.words.list:
                for seq in self.sequences:
                    #print("self.sequences is:", self.sequences)
                    # now if both og the current loop indices are 1 more than the indices stored in the SequenceList,
                    # then we append to the sequence object
                    if (word1_index == seq.word1_index + 1) and (word2_index == seq.word2_index + 1):


                        # if the words match strictly add them to the sequence
                        if wordmatch(word1, word2, 0):
                            seq.append(word1)
                            seq.word1_index = seq.word1_index + 1
                            seq.word2_index = seq.word2_index + 1


                        # now allowing for two spelling mistakes
                        elif wordmatch(word1, word2, self.allowed_spelling_mistakes):
                            seq.spelling_mistakes = True
                            seq.append(word1)
                            seq.word1_index = seq.word1_index + 1
                            seq.word2_index = seq.word2_index + 1
                        elif sequence_gaps < self.allowed_sequence_mistakes:
                            sequence_gaps += 1
                            seq.sequence_mistakes = True
                            seq.append(word1)
                            seq.word1_index = seq.word1_index + 1
                            seq.word2_index = seq.word2_index + 1

                # if identical then append on the stringmatch object level, not stringmatch string level.
                if wordmatch(word1, word2, self.allowed_spelling_mistakes):
                    self.sequences.append(SequenceList(word1_index, word2_index, [word1]))
                word2_index += 1
            word1_index += 1
            word2_index = 0

        # now we need to reduce sequences, i.e eliminate sequences that are subsets of other sequences
        reduced_sequences = self.sequences.copy()
        for seq1 in self.sequences:
            for seq2 in self.sequences:
                # if we are comparing literally the same object with itself, we pass.
                if seq1.list == seq2.list:
                    pass
                # if one string is a subset of the other
                elif set(seq1.list).issubset(set(seq2.list)):
                   # if the subsets indices lie within the indices of the superset
                   if ((seq1.s1_start_index >= seq2.s1_start_index and seq1.word1_index <= seq2.word1_index) and
                       (seq1.s2_start_index >= seq2.s2_start_index and seq1.word2_index <= seq2.word2_index)):
                       # if we have not already eliminated the subset from the reduced_sequences set
                        if seq1 in reduced_sequences:
                            reduced_sequences.remove(seq1)
                elif set(seq2.list).issubset(set(seq1.list)):
                    # if the subsets indices lie within the indices of the superset
                    if ((seq2.s2_start_index >= seq1.s2_start_index and seq2.word2_index <= seq1.word2_index) and
                            (seq2.s1_start_index >= seq1.s1_start_index and seq2.word1_index <= seq1.word1_index)):
                        # if we have not already eliminated the subset from the reduced_sequences set
                        if seq2 in reduced_sequences:
                            reduced_sequences.remove(seq2)
        self.sequences = reduced_sequences.copy()

        # now with reduced sequences we select the best matches if the is many-to-one matching
        # longer matches with spelling mistakes take precedence over matches without spelling mistakes
        # i.e. longer sequences take precedence
        # for matches of the same length, the one with fewer spelling mistakes takes precedence
        # for matches of same length and spelling mistakes, first one takes precedence

        filtered_sequences = self.sequences.copy()
        for seq1 in self.sequences:
            for seq2 in self.sequences:
                # if the same sequence is being compared with itself, pass
                if seq1 == seq2:
                    pass
                # if sequence 1 and sequence 2 share indices in a string i.e. overlap (entirely or subset)
                elif ((seq1.s1_start_index >= seq2.s1_start_index and seq1.word1_index <= seq2.word1_index) or
                        (seq1.s2_start_index >= seq2.s2_start_index and seq1.word2_index <= seq2.word2_index)):
                    # if sequence 1 is longer, remove sequence 2
                    if len(seq1.list) > len(seq2.list):
                        if seq2 in filtered_sequences:
                            filtered_sequences.remove(seq2)
                    # if sequence 2 is longer, remove sequence 1
                    elif len(seq2.list) > len(seq1.list):
                            if seq1 in filtered_sequences:
                                filtered_sequences.remove(seq1)
                    # if sequence 1 is same length as sequence 2
                    elif len(seq2.list) == len(seq1.list):
                        # if sequence 1 has sequence mistakes (gaps), but sequence 2 doesn't, then remove sequence 1
                        if seq1.sequence_mistakes == True and seq2.sequence_mistakes == False:
                            if seq1 in filtered_sequences:
                                filtered_sequences.remove(seq1)
                        # vice versa for sequence 2
                        elif seq1.sequence_mistakes == False and seq2.sequence_mistakes == True:
                            if seq2 in filtered_sequences:
                                filtered_sequences.remove(seq2)
                        # if both have sequence mistakes, then evaluate spelling mistakes
                        else:
                            # if sequence 1 has spelling mistakes, but sequence 2 doesn't, then remove sequence 1
                            if seq1.spelling_mistakes == True and seq2.spelling_mistakes == False:
                                if seq1 in filtered_sequences:
                                    filtered_sequences.remove(seq1)
                            elif seq1.spelling_mistakes == False and seq2.spelling_mistakes == True:
                                if seq2 in filtered_sequences:
                                    filtered_sequences.remove(seq2)
                            else:
                                # if the overlap is in string 1, remove the later match in string 2
                                if (seq1.s1_start_index >= seq2.s1_start_index and seq1.word1_index <= seq2.word1_index):
                                    if seq1.s2_start_index <= seq2.s2_start_index:
                                        if seq2 in filtered_sequences:
                                            filtered_sequences.remove(seq2)
                                    else:
                                        if seq1 in filtered_sequences:
                                            filtered_sequences.remove(seq1)
                                elif (seq1.s2_start_index >= seq2.s2_start_index and seq1.word2_index <= seq2.word2_index):
                                    if seq1.s1_start_index <= seq2.s1_start_index:
                                        if seq1 in filtered_sequences:
                                            filtered_sequences.remove(seq1)
                                    else:
                                        if seq2 in filtered_sequences:
                                            filtered_sequences.remove(seq2)
        self.sequences = filtered_sequences.copy()

        # now all words in matched sequences go into the "matched" attributes
        for seq in self.sequences:
            for i in range(seq.s1_start_index, seq.word1_index + 1):
                self.s1.matched.append(self.s1.words.list[i])
            for i in range(seq.s2_start_index, seq.word2_index + 1):
                self.s2.matched.append(self.s2.words.list[i])

        # now put everything that is not matched in the remainder
        self.s1.remainder.copy(self.s1.words)
        for word in self.s1.matched.list:
            if word in self.s1.remainder.list:
                self.s1.remainder.remove(word)
        self.s2.remainder.copy(self.s2.words)
        for word in self.s2.matched.list:
            if word in self.s2.remainder.list:
                self.s2.remainder.remove(word)



    def calculate_matches(self):
        self.s1.calculate_match()
        self.s2.calculate_match()

        # compute subsets
        if self.s1.percent_letters_matched == 100 or self.s2.percent_letters_matched == 100:
            self.subset = True
        else:
            self.subset = False

        # set the maximum word and letter match
        self.max_percent_letters_matched = max(self.s1.percent_letters_matched, self.s2.percent_letters_matched)
        self.max_percent_words_matched = max(self.s1.percent_words_matched, self.s2.percent_words_matched)



    def print_analysis(self):
        print('\nSTRING COMPARISON ANALYSIS')
        print("\nString 1:", self.s1.string, "\nString 2", self.s2.string)

        print("\nCleanString 1:", self.s1.clean_string, "\nCleanString 2", self.s2.clean_string)

        print("\nList 1:", self.s1.words.list, "\nString 2", self.s2.words.list)

        print("\nList 1: word count:", self.s1.words.word_count, "letter count:", self.s1.words.letter_count,
              "\nList 2: word count:", self.s2.words.word_count, "letter count:", self.s2.words.letter_count)

        print("\nSequence Report:\n")
        for seq in self.sequences:
            seq.print_sequence()


        print("\nMatched 1:", self.s1.matched.list, "\nMatched 2:", self.s2.matched.list)

        print("\nRemainder 1:", self.s1.remainder.list, "\nRemainder 2:", self.s2.remainder.list)

        print("\nMatched Weak 1:", self.s1.matched_weak.list, "\nMatched Weak 2:", self.s2.matched_weak.list)

        print("\nRemainder Weak 1:", self.s1.remainder_weak.list, "\nRemainder Weak 2:", self.s2.remainder_weak.list)

        print("\nMatch results String 1")
        print("Percent words:", self.s1.percent_words_matched, "weak:", self.s1.percent_words_matched_weak)
        print("Percent letters:", self.s1.percent_letters_matched, "weak:", self.s1.percent_letters_matched_weak)
        print("\nMatch results String 2")
        print("Percent words:", self.s2.percent_words_matched, "weak:", self.s2.percent_words_matched_weak)
        print("Percent letters:", self.s2.percent_letters_matched, "weak:", self.s2.percent_letters_matched_weak)

        print("\nsubset:", self.subset, "subset_weak:", self.subset_weak)
        print("\nmax percent words:", self.max_percent_words_matched, "\nmax percent words weak:",
              self.max_percent_words_matched_weak)
        print("max percent letters:", self.max_percent_letters_matched, "\nmax percent letters weak:",
              self.max_percent_letters_matched_weak)



def wordmatch(word1, word2, allowed_spelling_mistakes = 0):
    if allowed_spelling_mistakes == 0:
        if word1 == word2:
            return True
        else:
            return False

    matches = []
    l1 = list(word1)
    l2 = list(word2)
    if abs(len(l1) - len(l2)) <= allowed_spelling_mistakes:
        if len(l1) >= len(l2):
            shorter_list = l2
            longer_list = l1
        else:
            shorter_list = l1
            longer_list = l2
        diff = len(longer_list) - len(shorter_list)

        mistakes_combos = []
        for j in range(0, diff + 1): # loops from 0 to diff number
            start_index = j
            # print("new combo, start index", start_index)
            mistakes = 0
            for i in range(0, len(longer_list)):
                if i < start_index:
                    # print("buffer")
                    mistakes = mistakes + 1
                else:
                    try:
                        shorter_letter = shorter_list[i - start_index]
                    except:
                        shorter_letter = ""
                    try:
                        longer_letter = longer_list[i]
                    except:
                        longer_letter = ""
                    # print("words are:", word1, word2, "longer letter is", longer_letter, "short letter is", shorter_letter)
                    if longer_letter != shorter_letter:
                        mistakes = mistakes + 1

            # print("mistakes:", mistakes)
            if mistakes <= allowed_spelling_mistakes:
                mistakes_combos.append({'word1': word1, 'word2': word2,
                                        'mistakes': mistakes})

        mistake_combos = sorted(mistakes_combos, key=lambda i: i['mistakes'])
        if len(mistake_combos) > 0:
            matches.append(mistake_combos[0])

    #print("matches are candidates are:", matches)

    if len(matches) > 0:
        return True
    else:
        return False


    # now we sort the candidates with strongest first
    #weak_candidates = sorted(weak_candidates, key=lambda i: i['mistakes'])
    # print('weak candidates are', weak_candidates)

    # now we remove the strongest of the weak candidates in an interative loop
    # for c in weak_candidates:
    #     if c['word1'] in self.s1.remainder_weak.list and c['word2'] in self.s2.remainder_weak.list:
    #         self.s1.remainder_weak.remove(c['word1'])
    #         self.s2.remainder_weak.remove(c['word2'])
    #         self.s1.matched_weak.append(c['word1'])
    #         self.s2.matched_weak.append(c['word2'])


if __name__ == "__main__":
    # s1 = ""
    # s2 = ""
    # x = String_Comparison(s1, s2, 2)
    # x.print_analysis()
    #
    # s3 = ""
    # s4 = ""
    # x = String_Comparison(s3, s4, 2)
    # x.print_analysis()

    # s1 = ""
    # s2 = ""
    #s1 = ""
    #s2 = ""

    VR  = ""

    API  = ""


    x = StringComp(VR, API, 1, 0)
    x.print_analysis()

