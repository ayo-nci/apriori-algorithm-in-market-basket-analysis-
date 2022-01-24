#! /usr/bin/python3
# casol.py

import sys
from mrjob.job import MRJob, MRStep
from itertools import combinations
from casolm import FreqK

transaction_count = None
k_minus_one_itemsets = {}

def runjob(job,iteration):
     with job.make_runner() as runner:
          print("Running iteration {}\n".format(job.options.iteration))
          runner.run()
          if iteration == 1:
               global transaction_count
               counters = runner.counters()               
               transaction_count = counters[1]['association_rules']['transaction_count']
              # transaction_count = 1
          #fh = open(job.options.f,'w')
          for key, value in job.parse_output(runner.cat_output()):
            new_s = float(job.options.support) * (2.718**(float(job.options.decay) * iteration)) #added decay               
           #    if iteration >= int(job.options.k) - 1:
                    
            if value / transaction_count >= float(new_s):
                 #fh.write('{}\n'.format(key))
                 if iteration == 1:
                    yield key, value / transaction_count
                 else:
                    yield set(key), value / transaction_count         
      #fh.close()


if __name__ == '__main__':
     args = sys.argv[1:]
     job = FreqK(args + ['-iteration','1'])
     k = int(job.options.k)
     for i in range(1,k+1):
          job = FreqK(args = args+['-iteration',str(i)])
          results = runjob(job,iteration=i)
            
          if i == int(job.options.k) - 1:              
               for result in results:
                    k_minus_one_itemsets[frozenset(sorted(result[0]))] = result[1]   
          elif i == int(job.options.k):
               fh = open(job.options.f, "w")             
               for result in results:
                    lhs_items = combinations(result[0], int(job.options.k)-1)
                    for lhs_item in lhs_items:
                        try:
                            confidence = list(result)[1] / k_minus_one_itemsets[frozenset(sorted(lhs_item))]
                            support = result[1]

                            if confidence > float(job.options.c):
                                rhs_item = next(iter(set(result[0]).difference(lhs_item)),'')                               
                                output_string = "{}: ---> {} : support = {} : confidence = {}\n"
                                print(output_string.format(
                                  ",".join(lhs_item), 
                                  rhs_item, 
                                  support, 
                                  confidence))
                                fh.write(output_string.format(
                                  ",".join(lhs_item), 
                                  rhs_item, 
                                  support, 
                                  confidence))

                        except KeyError:
                             print("Set removed")                             
                             pass
               fh.close()

          else:
                
               fh = open(job.options.f, "w")
               new_s = float(job.options.support) * (2.718**(float(job.options.decay) * i))
               for result in results:
                    key = result[0]
                    value = result[1]
                    print("key,value", key, value)
                    if value >= float(new_s):
                        fh.write('{}\n'.format(key))                        
                        #if i == int(job.options.k):
                         #   if int(job.options.k) == 1:
                          #         print(key)
                           # else:
                            #       print(set(key))
               fh.close()
