
from main import DataStore

print("Click on \n1 for Create.\n2 for Read\n3 for Delete\n")

action = int(input())

if action==1:
    DataStore.create("key1","value1")
elif action==2:
    DataStore.read("key1")
elif action==3:
    DataStore.delete("key1")