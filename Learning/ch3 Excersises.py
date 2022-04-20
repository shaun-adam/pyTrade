def right_justify(str):
  length = len(str)
  spacers = 70-length  
  print(' '*spacers + str)

#right_justify('shaun')

# do_twice example

def do_twice(f,str):
    f(str)
    f(str)

def printer(tx):
    print(tx)

#do_twice(printer,'shaun')


