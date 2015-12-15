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
        "equipe" : int(attrs[0]),
        "unidade" : int(attrs[1]),
        "grupocampanha" : int(attrs[2]),
        "categoriaprof" : int(attrs[3]),
        "horaproducao" : int(attrs[4]),
        "diaproducao" : int(attrs[5]),
        "tipocontrib" : int(attrs[6]),
        "tipocobranca" : int(attrs[7]),
        "empresa" : int(attrs[8]),
        "tipopessoacontrib" : int(attrs[9]),
        "sexocontrib" : int(attrs[10]),        
        "idadecontrib" : 0, #int(0 if attrs[12] == '' else attrs[12]),
        "reputacaocontrib" : float(attrs[12]) * 1000,
        "reputacaoprof" : float(attrs[13]) * 1000,
        "mesestrabalhandoprof" : int(attrs[14]),
        "qtdproducoes" : int(attrs[15]),
        "qtdtipoproducoes" : int(attrs[16]),
        "valortotalproducoes" : float(attrs[17]),
        "valormedioproducoes" : float(attrs[18]),
        "mesesprimeiradoacao" : int(attrs[19]),
        "valoraceito" : float(attrs[20]),
        "valorsegmentado" : float(attrs[21]),
        "ligduracao" : int(attrs[22]),
        "gravduracao" : int(attrs[23]),
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
  parser.add_argument('--file', default="./data/data_training.csv")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)


