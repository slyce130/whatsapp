import sqlite3
from protobuf_decoder.protobuf_decoder import Parser

con = sqlite3.connect('ChatStorage.sqlite')
cur = con.cursor()

q = '''select msg.ztext, msgi.zreceiptinfo, msgi.z_pk, groupmem.zmemberjid from zwamessage msg 
join zwachatsession chat on msg.zchatsession = chat.z_pk
left join zwagroupmember groupmem on msg.zgroupmember = groupmem.z_pk
join zwamessageinfo msgi on msg.z_pk = msgi.zmessage 

where chat.zpartnername in ('Fluffy tummy squad', 'heavy and dense togetherly') 
and msg.zmessagetype != 46

order by msg.zmessagedate desc'''

# to test multiple reacts
# and msgi.z_pk = 106705

# to test react on self message
# and msg.z_pk = 203372


rows = cur.execute(q).fetchall()

message_reacts = {}
react_count = {}
top_message = {}
for row in rows:
    if row[1] == None:
        continue

    output = Parser().parse(row[1].hex())
    try:
        results = next(x for x in output.results if x.field == 7).data.results
    except:
        continue


    message_writer = row[3] or "self"
    message = row[0]


    for result in results:

        # find field == 2 in result.data.results
        if len(result.data.results) == 4:
            react_by = 'self'
            reaction = next((x.data for x in result.data.results if x.field == 2), 'self')
        else:
            react_by = next((x.data for x in result.data.results if x.field == 2), 'self')
            reaction = next((x.data for x in result.data.results if x.field == 3))

        if message_writer not in message_reacts:
            message_reacts[message_writer] = {}

        if message_writer not in react_count:
            react_count[message_writer] = 0

        if message not in top_message:
            top_message[message] = []

        if react_by not in message_reacts[message_writer]:
            message_reacts[message_writer][react_by] = []


        message_reacts[message_writer][react_by].append(reaction)
        react_count[message_writer] += 1
        top_message[message].append(reaction)


best_messages = sorted(top_message, key=lambda key: len(top_message[key]), reverse=True)
test = 0