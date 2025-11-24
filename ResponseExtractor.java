import org.springframework.http.client.ClientHttpResponse;
import java.io.IOException;

@FunctionalInterface
public interface ResponseExtractor<T> {
    T extractData(ClientHttpResponse response) throws IOException;
}
