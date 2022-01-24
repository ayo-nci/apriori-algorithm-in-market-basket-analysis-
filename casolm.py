#! /usr/bin/python3
# casolm.py

import sys, re
from mrjob.job import MRJob, MRStep
from itertools import combinations
from itertools import chain

    
class FreqK(MRJob):

    q_list = []
    
    def configure_args(self):
        super(FreqK, self).configure_args()
        self.add_passthru_arg('--support', default=0.1, help='Specify the minimum support threshold')
        self.add_passthru_arg('--k', default=3, help='Specify the k items to find')
        self.add_file_arg('--f', default='thefreq.txt', help='Specify the file used to store frequent itemsets')
        self.add_passthru_arg('--c', type=float, default=0.4, help="Specify the minimum confidence threshold")
        self.add_passthru_arg('--decay', type=float, default=-0.5, help="Specify the decay value")
        self.add_passthru_arg('-iteration', default='1', help='Enter the item sets you want. Default is 1')


    def steps(self):
        return [
            MRStep(
                   mapper=self.mapper_uo,
                   reducer=self.reducer_uo),
            MRStep(mapper_init=self.mapper_get_items_init,
                   mapper=self.mapper_get_items,
                   combiner=self.combiner_count_items,
                   reducer=self.reducer_total_items)
        ]

    def mapper_uo(self,_,line):
        if not str(line).__contains__("order_id"):
            lineitems = line.split(",")
            yield lineitems[0].strip('"'), lineitems[2].strip('"')
           
  
    def reducer_uo(self,key,val):
        yield key, ','.join(val)


    def mapper_get_items_init(self):
         if int(self.options.iteration) > 1:           
            with open(self.options.f,'r') as q:
                self.q_list = [line.strip().split() for line in q.readlines()]
                #self.q_list = set(q.read().splitlines())
         else:
            self.q_list = []

    def mapper_get_items(self,_,line):
        #res = re.search(r"\[.*?\]",line)[0].strip("[]").replace(" ", "").replace("'","")
        lineitems = set(line.split(","))

        if int(self.options.iteration) == 1:   
            self.increment_counter("association_rules", "transaction_count", 1)
            for item in set(lineitems):
                yield item.strip(), 1
        else:
            itpair = list(combinations(lineitems, int(self.options.iteration)))
            ispair = filter(lambda x:set(x) not in self.q_list, itpair)
            for sp in ispair:
                yield sp, 1

    def combiner_count_items(self,item,counts):
            yield item, sum(counts)

    def reducer_total_items(self,item,counts):
            yield item, sum(counts)




if __name__ == '__main__':
    FreqK.run()



