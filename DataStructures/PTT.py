class PTT: #Periodical Total Transaction

    ptt = {}

    def __init__(self, htg):
        for lLevel, maxPi in enumerate(htg):
            self.ptt[lLevel] = {}
            for pi in range(maxPi):
                self.ptt[lLevel][pi+1] = 0

    def addItemPeriodsToPtt(self, periods):
        # periods is an array of numbers, where each column is the l-level and the number is the pi in it. (eg. [1,30,10])
        for llevel, pi in enumerate(periods):
            if((llevel in self.ptt) and (pi in self.ptt[llevel]) ):
                self.ptt[llevel][pi] += 1
            else:
                print('ERROR AL AGREGAR ELEMENTO AL PTT. PERIODOS NO COMPATIBLE CON HTG')