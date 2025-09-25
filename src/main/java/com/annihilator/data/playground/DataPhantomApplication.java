package com.annihilator.data.playground;

import com.annihilator.data.playground.auth.DataPhantomAuthenticator;
import com.annihilator.data.playground.auth.DataPhantomUser;
import com.annihilator.data.playground.auth.JwtService;
import com.annihilator.data.playground.config.DataPhantomConfig;
import com.annihilator.data.playground.config.DataPhantomJwtConfig;
import com.annihilator.data.playground.db.MetaDBConnection;
import com.annihilator.data.playground.db.UserDAOImpl;
import com.annihilator.data.playground.resource.AuthResource;
import com.annihilator.data.playground.resource.DataPhantomResource;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.oauth.OAuthCredentialAuthFilter;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.db.DataSourceFactory;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import java.sql.SQLException;
import java.util.EnumSet;

public class DataPhantomApplication extends Application<DataPhantomConfig> {

    public static void main(String[] args) throws Exception {
        new DataPhantomApplication().run(args);
    }

    @Override
    public void initialize(Bootstrap<DataPhantomConfig> bootstrap) {

    }

    @Override
    public void run(DataPhantomConfig config, Environment environment) throws SQLException {

        DataSourceFactory dataSourceFactory = config.getMetaStore();
        dataSourceFactory.build(environment.metrics(), "database");

        final DataPhantomApplicationHealthCheck healthCheck = new DataPhantomApplicationHealthCheck();
        environment.healthChecks().register("app", healthCheck);

        JwtService jwtService = createJwtService(config.getJwt());
        environment.jersey().register(new AuthDynamicFeature(
                new OAuthCredentialAuthFilter.Builder<DataPhantomUser>()
                        .setAuthenticator(new DataPhantomAuthenticator(jwtService))
                        .setPrefix("Bearer")
                        .buildAuthFilter()
        ));

        // Create database connection and DAOs
        MetaDBConnection metaDBConnection = new MetaDBConnection(config.getMetaStore(), environment);
        UserDAOImpl userDAO = new UserDAOImpl(metaDBConnection);

        // Register resources
        final DataPhantomResource resource = new DataPhantomResource(config, environment);
        environment.jersey().register(resource);
        
        final AuthResource authResource = new AuthResource(userDAO, jwtService);
        environment.jersey().register(authResource);

        final FilterRegistration.Dynamic cors =
                environment.servlets().addFilter("CORS", CrossOriginFilter.class);

        cors.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "http://localhost:3000");
        cors.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "X-Requested-With,Content-Type,Accept,Origin,Authorization");
        cors.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "OPTIONS,GET,PUT,POST,DELETE,HEAD");
        cors.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM, "true");

        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

    private JwtService createJwtService(DataPhantomJwtConfig config) {
        return new JwtService(
                config.getSecretKey(),
                config.getTokenExpirationMinutes(),
                config.getRefreshTokenExpirationDays()
        );
    }
}
