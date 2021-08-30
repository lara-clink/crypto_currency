import docker
def airflow_build(dag_path):
    """
    This function runs the build for a container with airflow processes locally. Turns on webserver and scheduler.
    :param dag_path: (string) the filepath where dags live locally.
    :param test_path: (string) the filepath where the pytest script lives locally.
    :return: returns the docker container object.
    """
    client = docker.from_env()
    client.images.pull("apache/airflow")
    running_container = client.containers.run(
        "puckel/docker-airflow",
        detach=True,
        ports={"8080/tcp": 8080},  # expose local port 8080 to container
        volumes={
            dag_path: {"bind": "/usr/local/airflow/dags/", "mode": "rw"},
            # test_path: {"bind": "/usr/local/airflow/test/", "mode": "rw"},
            # requirements: {"bind": "/usr/local/airflow/requirements.txt", "mode": "rw"}
        },
    )
    running_container.exec_run(
        "airflow initdb", detach=True
    )  # docker execute command to initialize the airflow db
    running_container.exec_run(
        "airflow scheduler", detach=True
    )  # docker execute command to start airflow scheduler
    print(f'running container {running_container}')
    return running_container

airflow_build('/Users/lara.clink/Downloads/stone/dag_crypto.py')