package szp.rafael.kafka.accounttransaction.serde;

public class SerdeFactory<T> {

    public JSONSerdes<T> createSerde(Class<T> clazz){
        return new JSONSerdes<>(clazz);
    }

}

