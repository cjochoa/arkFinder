# -*- coding: utf-8 -*-
from __future__ import print_function
import re, codecs, json
from tinydb import TinyDB, Query

class IntelRecommendation(object):
    def __init__(self, path="db_R.json"):
        db          = TinyDB(path)
        self.db     = db.table("cpus")
        self.keys   = db.table("keys")
        self.intel  = Query()

    def _normalize(self, text):
        return re.sub(r'[^\w\s]','',text.lower())

    def set_key(self, entry):
        tmp = {}
        for k,v in entry.iteritems():
            if k == "Property Name":
                tmp["subject"] = v
            elif k == "Key":
                tmp["_subject"] = self._normalize(v)
            else:
                tmp[k] = v
        if tmp.get("subject", None):
            self.keys.insert(tmp)
        return True

    def set_data(self, entry):
        """
        Args:
            entry (dict): the content to store
        Returns:
            always True for now...
        """
        self.db.insert(entry)
        return True

    def getAllQueries(self):
        res = []
        for r in self.keys.all():
            res.append((r["_subject"], r["subject"]))
        return res

    def getDefinition(self, term, nbest=-1):
        term  = reduce(lambda a,b: a&b, map(lambda w: self.intel._subject.search(w), self._normalize(term).split()))
        query = self.keys.search(term)
        nbest = nbest if nbest>0 else len(query)
        return query[:nbest]

    def getQuery(self, terms):
        """
        Args:
            terms (dict): search query
        Returns:
            dict with normalized keys
        """
        search_query = []
        for key, value in terms.items():
            term  = reduce(lambda a,b: a&b, map(lambda w: self.intel._subject.search(w), self._normalize(key).split()))
            query = self.keys.search(term)    
            tmp   = []
            for q in query:
                if q["subject"] and value:
                    tmp.append(self.intel[q["subject"]].search(value))
            if len(tmp)>0:
                search_query.append(reduce(lambda a,b: a|b, tmp))
        if len(search_query)>0:
            return reduce(lambda a,b: a&b, search_query)
        return None

    def getRecommendation(self, terms, nbest=-1):
        """
        Args:
            terms (dict): search query
            nbest (integer) : default is -1 and results in maximal results
        Returns:
            list: list of dictionaries of results
        """
        if terms:
            result = self.db.search(terms)
            return result
        return []

def load_data():
    IR      = IntelRecommendation()
    # Enter Keys
    stage   = None
    tmp     = {}
    data    = []
    for line in open("processors.txt", 'r'):
        line = line.splitlines()[0]
        if len(line.split(':'))>1:
            tmp[line.split(':')[0].strip()] = line.split(':')[1].strip()
        else:
            if stage:
                data.append(tmp)
                tmp = {}
            tmp["Key"] = line.strip()
            stage = line
    for d in data:
        IR.set_key(d) 
    # Enter data
    data = json.load(open("cpu_10k.json", 'r'))["d"]
    for i in range(len(data)):
        tmp = {}
        for k,v in data[i].items():
            if type(v)!=type({}):
                tmp[k.strip()] = ("%s"%v).strip()
        IR.set_data(tmp)
        if (i+1)%100==0:
            print("processed", i+1)
    print("done.")

def examples():
    intel = IntelRecommendation()
    def test(query):
        result = intel.getRecommendation(query)
        print("#results:",len(result))
        for res in result[:min(5,len(result))]:
            print("%50s"%"cpu:",res["Link"])

    print("you can look for following things (the left side is your query):")
    for q in intel.getAllQueries():
        print("%80s:%s"%(q[0], q[1]))
    print()
    print()
    print("this is yet another source of descriptions, e.g. for QA:")
    for e in intel.getDefinition("Execute Disable Bit"):
        for k,v in e.iteritems():
            print(k, ' : ',v)
    print()
    print()
    print("this is how you can look for data:")
    cmd = []
    cmd.append({"cpu cores":"1"})
    cmd.append({"Execute Disable Bit":"True"})
    cmd.append({"cores":"1", "Execute Disable Bit":"True"})
    cmd.append({"cores":"4", "Execute Disable Bit":"True", "graphic cores":"1"})
    cmd.append({"4k":"Yes", "Execute Disable Bit":"True"})
    cmd.append({"Threads":"12", "Processor Base Frequency":"3.7"})
    cmd.append({"Execute Disable Bit":"True","Threads":"12", "Processor Base Frequency":"3.7"})
    for c in cmd:
        query   = intel.getQuery(c)
        print("Ref. query :",c)      # this is the query from the user
        print("Norm. query:",query)  # this is a query-proposal retrieved from data
        test(query)                 # this is the search result based on the proposed query
        print()
    
if __name__ == "__main__":
#    load_data()
    examples()

