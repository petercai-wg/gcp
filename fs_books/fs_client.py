from google.cloud import firestore

# get the client
db = firestore.Client()

# create a new document
data = {"name": "Sue", "role": "treasurer"}
member_ref = db.collection("members").document()
member_ref.set(data)
member_id = member_ref.get().id

# retrieve a document
member_ref = db.collection("members").document(member_id)
member = member_ref.get()
if member.exists:
    print(f"Document data: {member.to_dict()}")
else:
    print("Member not found.")

# update a document
new_data = {"name": "Sue", "role": "president"}
member_ref = db.collection("members").document(member_id)
member_ref.set(new_data)

# get all documents in order
members = db.collection("members").order_by("name").stream()
for member in members:
    print(f"{member.id} => {member.to_dict()}")

# delete a member
member_ref = db.collection("members").document(member_id)
member_ref.delete()
