package study.practice.reactivepractice;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SpringBootTest
public class ReactorTest {

    @Test
    void testError() {
        Flux<String> flux = Flux.just("thing1", "thing2") // 제공된 배열 요소를 포함한 Flux를 만든다.
            .concatWith(Mono.error(new IllegalArgumentException("boom"))) // Mono Type으로 Error를 붙여준다.
            .log();

        StepVerifier.create(flux)
            .expectNext("thing1")
            .expectNext("thing2")
            .expectError()
            .verify(); // verify를 하지 않으면 Flux가 생기지 않고, 시퀀스가 흘러가지도 않는다.
    }

    @Test
    void testBackPressure() {
        Flux.just(1, 2, 3, 4) // 4개짜리 Flux를 만든다.
            .log()
            .subscribe(new Subscriber<>() { // Subscriber가 구독할 것이다.
                private Subscription subscription;
                int onNextAmount;

                @Override
                public void onSubscribe(Subscription s) { // 신호를 잡아서 처리한다.
                    this.subscription = s;
                    s.request(2); // request를 2로 잡았다 = 한 번에 2개씩만 잡겠다.
                }

                @Override
                public void onNext(Integer integer) {
                    onNextAmount++;

                    if (onNextAmount % 2 == 0) { // Amount가 2번씩 돌아올 때 마다
                        subscription.request(2); // request를 2개씩만 잡겠다.
                    }
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onComplete() {
                }
            });
    }

    @Test
    void testMap() {
        Flux<Integer> flux = Flux.just(1 ,2, 3); // 1. 원본 Flux 시퀀스들이 흘러가고 있다.
        Flux<Integer> flux2 = flux.map(i -> i * 10) // 2. map에 의해 새로운 시퀀스를 만든다.
            .log();

        StepVerifier.create(flux2)
            .expectNext(10, 20, 30) // map 결과 10, 20, 30이 나온다고 기대를 할 수 있다.
            .verifyComplete();
    }

    @Test
    void testFlatMap() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10)) // flatMap 같은 경우에는 따로 publisher가 감싸주지 않기 때문에 Mono.just()를 사용한다.
            .log();

        StepVerifier.create(flux2)
            .expectNextCount(flux.count().block())
            .verifyComplete();
    }

    @Test
    void testScheduler_subscribeOn_boundedElastic() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .subscribeOn(Schedulers.boundedElastic())
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .thenConsumeWhile(item -> item > 0)
                .verifyComplete();
        }
    }

    @Test
    void testScheduler_publishOn_parallel() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .publishOn(Schedulers.parallel())
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .thenConsumeWhile(item -> item > 0)
                .verifyComplete();
        }
    }

    @Test
    void testScheduler() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .thenConsumeWhile(item -> item > 0)
                .verifyComplete();
        }
    }

    @Test
    void testScheduler_publishOn_single() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .publishOn(Schedulers.single())
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .thenConsumeWhile(item -> item > 0)
                .verifyComplete();
        }
    }

    @Test
    void testScheduler_publishOn_boundedElastic() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .publishOn(Schedulers.boundedElastic())
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .thenConsumeWhile(item -> item > 0)
                .verifyComplete();
        }
    }

    @Test
    void testScheduler_firstPublishOn_boundedElastic_secondPublishOn_parallel() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .publishOn(Schedulers.boundedElastic())
            .filter(i -> i / 10 == 2) // 20 값만 허용
            .publishOn(Schedulers.parallel())
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .thenConsumeWhile(item -> item > 0)
                .verifyComplete();
        }
    }

    @Test
    void testScheduler_expectNext() {
        Flux<Integer> flux = Flux.just(1, 2, 3);
        Flux<Integer> flux2 = flux.flatMap(i -> Mono.just(i * 10))
            .publishOn(Schedulers.boundedElastic())
            .filter(i -> i / 10 == 2) // 20 값만 허용
            .publishOn(Schedulers.parallel())
            .log();

        for (int i = 0; i < 10; i++) { // 10번 반복

            StepVerifier.create(flux2)
                .expectNext(20) // 20 값만 들어오는지 확인
                .verifyComplete();
        }
    }

    @Test
    void testZip() {
        Flux<Tuple2<Integer, Integer>> flux = Flux.just(1, 2, 3, 4)
            .log()
            .map(i -> i * 2) // map에 걸어서 값을 바꿔주고
            .zipWith(Flux.range(0, Integer.MAX_VALUE)) // zipWith를 하면
            .log();

        StepVerifier.create(flux)
            .expectNextCount(4) // 4개짜리 Flux tuple이 만들어질 것이다.
            .verifyComplete();
    }

    @Test
    void testZip_sameResult_byMap() {
        Flux<Mono<Tuple2<Integer, Flux<Integer>>>> flux = Flux.just(1, 2, 3, 4)
            .log()
            .map(i -> Mono.zip(Mono.just(i), Mono.just(Flux.range(0, Integer.MAX_VALUE)))) // map에 걸어서 값을 바꿔주고
//            .zipWith(Flux.range(0, Integer.MAX_VALUE)) // zipWith를 하면
            .log();

        StepVerifier.create(flux)
            .expectNextCount(4) // 4개짜리 Flux tuple이 만들어질 것이다.
            .verifyComplete();
    }

    @Test
    void testOperations() {
        List<Integer> integerList = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < 100; i++) { // 100번 반복
            integerList.add(random.nextInt(10)); // 랜덤된 10까지의 숫자 반영
        }

        Flux<Integer> flux = Flux.fromIterable(integerList); // Flux.fromIterable()은 자주 쓰인다.

        flux.groupBy(i -> i % 10) // key: predicate value, value : 통째로
            .doOnNext(g ->
                System.out.println(g.key() + ": " + g.collectList())
            ).subscribe();
    }
}
