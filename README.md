#Raciocínio do teste do processo para  Analista de Dados PL Magalu Bank[


#Fluxograma do projeto:

A ideia é utilizar spark para ler os json, passar os schemas, fazer os explodes e salvar em tabelas parquet, após isso subir as tabelas para o mysql e fazer os joins.
Infelizmente a máquina que eu possuía no magalu cloud não aguentou fazer os joins no próprio mysql, por isso subi a tabela para o mysql já cruzada, com as colunas que usarei no dahs e com o filtro de 3 meses a partir da última data de review.

No spark utilizei a modulação para chamar funções grandes que seriam repetidas várias vezes, por exemplo o SparkSession, para buildar a aplicação seria necessário 6 linhas, com a modulação ao apenas importar o arquivo para o script eu já buildo ele, o getOrCreate() me garante que ele não irá criar várias instâncias cada vez que usar a função importada.

Utilizei uma pasta chamada Bucket como Bucket, nele me baseei no LakeHouse do Magalu, transient somente copiei os jsons para la, raw passei os schemas, como os dados são de uma “ingestão” única, não criei a trusted que é onde se costuma deduplicar os datasets, com isso fiz scripts que enviam os dados para o mysql, nos parâmetros do envio setei o rewriteBatchedStatements como verdadeiro para realizar insert de várias linhas, com isso consegui acelerar o desempenho da gravação no banco de dados.(O Mysql connector, inspeciona o max_allowed_packet definido no banco, então não temos ricos de oneração)

Credenciais Mysql: 
Host: 177.93.135.139
Usuário: teste:labs
Senha: 1998kai@
Banco: teste_labs
Para conseguir acessar preciso que me passe o IP para liberar privilégios ao banco e acesso a porta da cloud.

Após os envios dos dados para o banco, dei os privilégios para que o looker studio consiga ler dados do mysql.

#Dashboard : 
https://lookerstudio.google.com/reporting/2132ae90-36a1-4c4e-bc1b-8cfa5694a111

No dashboard eu foquei na Business Review, tentei colocar informações de um jeito que consigamos analisar o desempenho das empresas de acordo com os reviews dos clientes.

#Apresentação : 
