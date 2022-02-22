class PTT:  # Periodical Total Transaction
    # A list of lists. Outer list is l-levels (Hardcoded in 3 elements). Inner lists are the amount of transactions contained in each time period contained in each level.
    # They are initialized in 0. So if your HTG is [4,2,5] the ptt created will be [[0,0,0,0], [0,0], [0,0,0,0,0]]

    def __init__(self):
        hardcoded_htg = [24, 12, 4]
        self.ptt = [[], [], []]
        for lLevel, maxPi in enumerate(hardcoded_htg):
            for x in range(maxPi):
                self.ptt[lLevel].append({'itemsSet': set(), 'totalTransactions': 0})

    def addMultiplePeriods(self, periods):
        for period in periods:
            self.addItemPeriodToPtt(period)

    def addItemPeriodToPtt(self, period, items = set()):
        # periods is an array of numbers, where each column is the l-level and the number is the pi in it. (eg. [1,
        # 30,10])
        for l_level, pi in enumerate(period):
            if self.checkIfLlevelAndPeriodAreValid(l_level, pi):
                self.ptt[l_level][pi - 1]['itemsSet'].update(items)
                self.ptt[l_level][pi - 1]['totalTransactions'] += 1
            else:
                print('ERROR AL AGREGAR ELEMENTO AL PTT. PERIODOS NO COMPATIBLE CON HTG')

    def getPTTValueFromLlevelAndPeriod(self, l_level, pi):
        if self.checkIfLlevelAndPeriodAreValid(l_level, pi-1):
            return self.ptt[l_level][pi - 1]
        else:
            print('ERROR AL OBTENER ELEMENTO EN LA PTT. EL NIVEL O PERIODO ASOCIADO NO EXISTE')

    def checkIfLlevelAndPeriodAreValid(self, l_level, period):
        return (len(self.ptt) >= l_level) and (len(self.ptt[l_level]) >= period)
