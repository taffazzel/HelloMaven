__author__ = 'tafaz'


class Calculation:
    def __init__(self):
        self.base = ''
        self.calc_list = []

    def connect(self,user):
        self.user = user
        print self.user

    def plus(self,a,b):
        self.calc_list.append(a+b)
        return a+b

    def minus(self,a,b):
        print("I am in cla minus")
        self.calc_list.append(a-b)
        return a-b

    def divide(self,a,b):
        try:
            z = a/b
            self.calc_list.append(z)
            return z
        except:
            ArithmeticError

    def show_list(self):
        print self.calc_list

