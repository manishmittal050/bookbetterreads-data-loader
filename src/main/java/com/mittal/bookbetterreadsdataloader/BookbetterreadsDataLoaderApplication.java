package com.mittal.bookbetterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.mittal.bookbetterreadsdataloader.author.Author;
import com.mittal.bookbetterreadsdataloader.author.AuthorRepository;
import com.mittal.bookbetterreadsdataloader.book.Book;
import com.mittal.bookbetterreadsdataloader.book.BookRepository;
import com.mittal.bookbetterreadsdataloader.connection.DataStaxAstraProperties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookbetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	public static void main(String[] args) {
		SpringApplication.run(BookbetterreadsDataLoaderApplication.class, args);
	}

	@Value("${datadump.location.author}")
	private String dataLocationActors;

	@Value("${datadump.location.works}")
	private String dataLocationWorks;

	private void initAuthor() {

		Path path = Paths.get(dataLocationActors);

		try (Stream<String> lines = Files.lines(path)) {

			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					authorRepository.save(author);
				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void initWorks() {
		Path path = Paths.get(dataLocationWorks);
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

		try (Stream<String> lines = Files.lines(path)) {

			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));

					book.setName(jsonObject.optString("title"));
					JSONObject desObj = jsonObject.optJSONObject("description");
					if (desObj != null)
						book.setDescription(desObj.optString("value"));

					JSONObject publicshedDateObj = jsonObject.optJSONObject("created");
					if (publicshedDateObj != null) {
						String dateStr = publicshedDateObj.optString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormatter));
					}

					JSONArray coversArray = jsonObject.optJSONArray("covers");
					if (coversArray != null) {
						List<String> covers = new ArrayList<>();
						for (int i = 0; i < coversArray.length(); i++) {
							covers.add(coversArray.getString(i));
						}
						book.setCoverIds(covers);
					}

					JSONArray authorsJSOnArray = jsonObject.optJSONArray("authors");
					if (authorsJSOnArray != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorsJSOnArray.length(); i++) {
							String authorId = authorsJSOnArray.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);

						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknown author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);

						System.out.println("Saving book into repository " + book.getName());
						bookRepository.save(book);

					}

				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	private void start() {
		System.out.println("This should print after project building is done " + dataLocationActors);
		// initAuthor();
		initWorks();

	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
