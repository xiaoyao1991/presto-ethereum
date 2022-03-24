if [[ "$1" == '' ]] ; then
  echo "Please provide the path to the root of your Presto server, e.g. /opt/presto/"
  exit
fi

mvn clean package
rm -rf $1/plugin/tezos
mkdir $1/plugin/tezos
tar xfz target/presto-tezos-1.0-SNAPSHOT-plugin.tar.gz -C $1/plugin/tezos --strip-components=1
