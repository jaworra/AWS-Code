import numpy as np
#set large number of trials
n_trial = 200_000

#collect counts here
ace_list = []
#assume ace is card value equals 1
target = 1

for _ in range (n_trial):
    
    #reinitialize counts at start of every for loop
    counts = 0
    draw = 0
    
    #while we don't hit target
    while draw != target:
    
        #draw card from 0 to 13
        draw = np.random.randint(1, 14)
        #update count every draw
        counts +=1
    #keep track of how many draws
    ace_list.append(counts)
    
#list comprehension to keep only 10 attemps or more
ace_tens = [x for x in ace_list if x>=10]
#answer
p_ace_tens = len(ace_tens)/n_trial
print(p_ace_tens)