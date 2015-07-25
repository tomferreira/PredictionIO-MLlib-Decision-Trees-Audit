"""
Import sample data for classification engine
"""

import predictionio
import argparse

def import_events(client, file):
  f = open(file, 'r')
  count = 0
  print "Importing data..."
  for line in f:
    data = line.rstrip('\r\n').split(",")
    resultado = data[0]
    attrs = data[1:]
    client.create_event(
      event="$set",
      entity_type="auditoria",
      entity_id=str(count), # use the count num as audit ID
      properties= {
        "profissional" : int(attrs[0]),
        "equipe" : int(attrs[1]),
        "unidade" : int(attrs[2]),
        "empresa" : int(attrs[3]),
        "valor" : float(attrs[4]),
        "gduracao" : int(attrs[5]),
        "resultado" : int(resultado)
      }
    )
    count += 1
  f.close()
  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for classification engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/sample_decision_trees.txt")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)
