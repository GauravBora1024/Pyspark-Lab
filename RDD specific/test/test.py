string = "the fox is  an animal"
new_str = ""
for i in range(len(string)-1,-1,-1):
    new_str+= string[i]
print(new_str)